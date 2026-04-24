import { useState } from "react";
import { open } from "@tauri-apps/plugin-dialog";

import { runIdentification } from "../lib/api";

type BatchRunPanelProps = {
  onLog: (line: string) => void;
  onCancel: () => void;
  isBuildingDatabase: boolean;
  onRunStart: () => void;
  onRunFailure: () => void;
  onSuccessNotice: (message: string) => void;
};

export function BatchRunPanel({
  onLog,
  onCancel,
  isBuildingDatabase,
  onRunStart,
  onRunFailure,
  onSuccessNotice
}: BatchRunPanelProps) {
  const [queryPaths, setQueryPaths] = useState<string[]>([]);

  const startDisabled = isBuildingDatabase;
  const startTitle = isBuildingDatabase
    ? "数据库正在构建中，请稍候"
    : undefined;

  return (
    <section className="sidebar-card query-panel-card" data-testid="query-panel-card">
      <h2>查询文件</h2>
      <p className="sidebar-copy">
        选择 `.mgf` 或 `.txt` 查询文件后启动批量鉴定任务。
      </p>
      <p className="status-line">
        {queryPaths.length === 0
          ? "尚未选择查询文件。"
          : `已选择 ${queryPaths.length} 个查询文件`}
      </p>
      <div className="panel-actions">
        <button
          type="button"
          onClick={async () => {
            const selection = await open({
              multiple: true,
              directory: false,
              filters: [
                {
                  name: "Mass Spectra",
                  extensions: ["mgf", "txt"]
                }
              ]
            });

            const paths = Array.isArray(selection)
              ? selection.filter((value): value is string => typeof value === "string")
              : typeof selection === "string"
                ? [selection]
                : [];

            if (paths.length === 0) {
              onLog("已取消选择查询文件。");
              return;
            }

            setQueryPaths(paths);
            onLog(`已选择 ${paths.length} 个查询文件。`);
            onSuccessNotice(`查询文件加载完成 · 共 ${paths.length} 个文件`);
          }}
        >
          选择查询文件
        </button>
        <button
          type="button"
          disabled={startDisabled}
          title={startTitle}
          aria-describedby={
            isBuildingDatabase ? "batch-run-blocked-hint" : undefined
          }
          onClick={async () => {
            if (queryPaths.length === 0) {
              onLog("请先选择查询文件，再启动批量鉴定。");
              return;
            }

            try {
              onRunStart();
              await runIdentification(queryPaths);
              onLog("批量鉴定已启动。");
            } catch (error) {
              onRunFailure();
              onLog(`启动批量鉴定失败：${String(error)}`);
            }
          }}
        >
          开始批量鉴定
        </button>
        <button type="button" onClick={onCancel}>
          取消
        </button>
      </div>
      {isBuildingDatabase ? (
        <p className="panel-hint" id="batch-run-blocked-hint" role="note">
          <span className="panel-hint-dot" aria-hidden="true" />
          数据库正在构建中，批量鉴定将在构建完成后可用。
        </p>
      ) : null}
    </section>
  );
}
