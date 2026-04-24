import { useState } from "react";
import { open, save } from "@tauri-apps/plugin-dialog";

import {
  buildDatabaseFromSource,
  exportDatabaseToCsv,
  openDatabase
} from "../lib/api";
import { ErrorCallout } from "./ErrorCallout";

type DatabasePanelProps = {
  onLog: (line: string) => void;
  isBuildingDatabase: boolean;
  onBuildStart: () => void;
  onBuildEnd: () => void;
  onSuccessNotice: (message: string) => void;
};

type PanelError = {
  title: string;
  message: string;
};

function summarizeDatabaseError(raw: string): PanelError {
  if (/magic|schema|invariant|layout|checksum|crc|too small|overflow/i.test(raw)) {
    return { title: "运行时格式校验失败", message: raw };
  }
  if (/no such file|failed to open|failed to mmap|permission denied/i.test(raw)) {
    return { title: "数据库目录不可读", message: raw };
  }
  return { title: "加载数据库失败", message: raw };
}

function summarizeBuildError(raw: string): PanelError {
  if (/missing required all\.db/i.test(raw)) {
    return { title: "未找到 all.db 源文件", message: raw };
  }
  if (/decode metadata|decode.*pickle|stream all\.db/i.test(raw)) {
    return { title: "原始数据解码失败", message: raw };
  }
  return { title: "构建运行时数据库失败", message: raw };
}

export function DatabasePanel({
  onLog,
  isBuildingDatabase,
  onBuildStart,
  onBuildEnd,
  onSuccessNotice
}: DatabasePanelProps) {
  const [databasePath, setDatabasePath] = useState<string>();
  const [errorState, setErrorState] = useState<PanelError | null>(null);

  const clearError = () => setErrorState(null);

  return (
    <section className="sidebar-card">
      <h2>数据库</h2>
      <p className="sidebar-copy">
        加载现成的 mmap 数据库，或从原始目录自动查找 `.db` 文件并构建。
      </p>
      <p className="status-line">
        {databasePath ? `已加载数据库：${databasePath}` : "尚未加载数据库。"}
      </p>
      <div className="panel-actions">
        <button
          type="button"
          disabled={isBuildingDatabase}
          title={isBuildingDatabase ? "数据库正在构建中，请稍候" : undefined}
          onClick={async () => {
            const selection = await open({
              directory: true,
              multiple: false
            });

            if (typeof selection !== "string") {
              onLog("已取消选择数据库目录。");
              return;
            }

            clearError();
            try {
              const startedAt = Date.now();
              await openDatabase(selection);
              setDatabasePath(selection);
              onLog(`已加载数据库：${selection}`);
              onSuccessNotice(
                `导入数据成功 · 已加载数据库 · 用时 ${(
                  (Date.now() - startedAt) /
                  1_000
                ).toFixed(2)} 秒`
              );
            } catch (error) {
              const raw = String(error);
              const summary = summarizeDatabaseError(raw);
              setErrorState(summary);
              onLog(`${summary.title}：${raw}`);
            }
          }}
        >
          选择数据库目录
        </button>
        <button
          type="button"
          disabled={isBuildingDatabase}
          title={isBuildingDatabase ? "数据库正在构建中，请稍候" : undefined}
          onClick={async () => {
            const sourceSelection = await open({
              directory: true,
              multiple: false
            });

            if (typeof sourceSelection !== "string") {
              onLog("已取消选择原始数据库目录。");
              return;
            }

            const outputSelection = await open({
              directory: true,
              multiple: false
            });

            if (typeof outputSelection !== "string") {
              onLog("已取消选择运行时数据库输出目录。");
              return;
            }

            clearError();
            onBuildStart();
            onLog(`开始构建运行时数据库：${sourceSelection} -> ${outputSelection}`);
            try {
              const builtPath = await buildDatabaseFromSource(
                sourceSelection,
                outputSelection
              );
              setDatabasePath(builtPath);
              onLog(`已构建并加载数据库：${builtPath}`);
            } catch (error) {
              const raw = String(error);
              const summary = summarizeBuildError(raw);
              setErrorState(summary);
              onLog(`${summary.title}：${raw}`);
            } finally {
              onBuildEnd();
            }
          }}
        >
          导入原始数据库目录并构建
        </button>
        <button
          type="button"
          disabled={isBuildingDatabase}
          title={isBuildingDatabase ? "数据库正在构建中，请稍候" : undefined}
          onClick={async () => {
            const destination = await save({
              defaultPath: "gpmsdb-export.csv",
              filters: [
                {
                  name: "CSV",
                  extensions: ["csv"]
                }
              ]
            });

            if (typeof destination !== "string") {
              onLog("已取消导出数据库 CSV。");
              return;
            }

            clearError();
            try {
              const startedAt = Date.now();
              await exportDatabaseToCsv(destination);
              onLog(`数据库 CSV 已导出到 ${destination}`);
              onSuccessNotice(
                `导出数据库 CSV 成功 · 用时 ${(
                  (Date.now() - startedAt) /
                  1_000
                ).toFixed(2)} 秒`
              );
            } catch (error) {
              const raw = String(error);
              setErrorState({ title: "导出数据库 CSV 失败", message: raw });
              onLog(`导出数据库 CSV 失败：${raw}`);
            }
          }}
        >
          导出数据库 CSV
        </button>
      </div>
      {errorState ? (
        <ErrorCallout
          title={errorState.title}
          message={errorState.message}
          onDismiss={clearError}
        />
      ) : null}
    </section>
  );
}
