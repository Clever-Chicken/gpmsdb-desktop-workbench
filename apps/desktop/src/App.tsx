import { useEffect, useRef, useState } from "react";
import { listen } from "@tauri-apps/api/event";

import { BatchRunPanel } from "./components/BatchRunPanel";
import { reduceBuildStatus, type BuildStatus } from "./components/BuildStatusBanner";
import {
  CompletionToasts,
  type CompletionNotice
} from "./components/CompletionToasts";
import { DatabasePanel } from "./components/DatabasePanel";
import { LogConsole } from "./components/LogConsole";
import {
  ProcessingStatusPanel,
  type ProcessingStatus
} from "./components/ProcessingStatusPanel";
import { ResultTable } from "./components/ResultTable";
import type {
  BatchProgressEvent,
  BatchResultRow,
  BatchResultsEvent,
  BuildProgressEvent
} from "./lib/events";

const MIN_PROCESSING_VISUAL_MS = 1_000;
const COMPLETION_NOTICE_MS = 4_200;

const idleProcessingStatus: ProcessingStatus = {
  stateLabel: "等待开始",
  headline: "尚未开始数据库处理或批量鉴定",
  summary: "选择数据库并加载查询文件后开始。",
  animate: false
};

function formatCount(value: number): string {
  return value.toLocaleString("en-US");
}

function summarizeBuildStatus(status: BuildStatus): string {
  switch (status.stage) {
    case "start":
      return status.allDb ? `数据源：${status.allDb}` : "正在准备构建所需数据。";
    case "genome":
      return status.stageDetail
        ? `当前基因组：${status.stageDetail} · 已处理 ${formatCount(status.genomesProcessed)} 个基因组，${formatCount(status.peaksProcessed)} 个峰值。`
        : `已处理 ${formatCount(status.genomesProcessed)} 个基因组，${formatCount(status.peaksProcessed)} 个峰值。`;
    case "indexStart":
      return "流式解码完成，正在整理 mass_index.bin。";
    case "metaStart":
      return "倒排索引已写入，正在整理 meta.bin。";
    case "finalizeStart":
      return "正在计算校验信息并完成收尾。";
    case "done":
      return `共处理 ${formatCount(status.genomesProcessed)} 个基因组，${formatCount(status.peaksProcessed)} 个峰值。`;
  }
}

function toBuildProcessingStatus(status: BuildStatus): ProcessingStatus {
  return {
    stateLabel: status.stage === "done" ? "已处理" : "处理中",
    headline: status.stageLabel,
    summary: summarizeBuildStatus(status),
    animate: status.stage !== "done"
  };
}

function toBatchProcessingStatus(progress: BatchProgressEvent): ProcessingStatus {
  return {
    stateLabel: "处理中",
    headline: `已处理 ${progress.processed} / ${progress.total}`,
    summary: progress.currentFile
      ? `当前文件：${progress.currentFile}`
      : "正在等待当前文件信息。",
    animate: true
  };
}

function toResultProcessingStatus(
  rows: BatchResultRow[],
  progress: BatchProgressEvent | null
): ProcessingStatus {
  const headline =
    rows.length === 0 ? "查询已结束，未返回匹配结果" : `查询已返回 ${rows.length} 条结果`;
  const summary = progress
    ? progress.currentFile
      ? `最新完成文件：${progress.currentFile}`
      : `已处理 ${progress.processed} / ${progress.total}`
    : "结果已更新到结果表。";

  return {
    stateLabel: "查询完成",
    headline,
    summary,
    animate: false
  };
}

function formatDuration(durationMs: number): string {
  return `${(durationMs / 1_000).toFixed(2)} 秒`;
}

function formatCompactDuration(durationMs: number): string {
  return `${(durationMs / 1_000).toFixed(2)}s`;
}

function App() {
  const [progressText, setProgressText] = useState("已处理 0 / 0");
  const [logs, setLogs] = useState<string[]>([]);
  const [results, setResults] = useState<BatchResultRow[]>([]);
  const [processingStatus, setProcessingStatus] = useState<ProcessingStatus>(
    idleProcessingStatus
  );
  const [completionNotices, setCompletionNotices] = useState<CompletionNotice[]>([]);
  const [isBuildingDatabase, setIsBuildingDatabase] = useState(false);
  const buildStatusRef = useRef<BuildStatus | null>(null);
  const batchProgressRef = useRef<BatchProgressEvent | null>(null);
  const buildStartedAtRef = useRef<number | null>(null);
  const buildIndexStartedAtRef = useRef<number | null>(null);
  const buildMetaStartedAtRef = useRef<number | null>(null);
  const batchStartedAtRef = useRef<number | null>(null);
  const processingStartedAtRef = useRef<number | null>(null);
  const processingCompletionTimerRef = useRef<number | null>(null);
  const processingIsAnimatingRef = useRef(false);
  const completionNoticeTimersRef = useRef<Map<number, number>>(new Map());

  const appendLog = (line: string) => {
    setLogs((current) => [...current, line]);
  };

  const dismissCompletionNotice = (id: number) => {
    const timer = completionNoticeTimersRef.current.get(id);
    if (timer !== undefined) {
      window.clearTimeout(timer);
      completionNoticeTimersRef.current.delete(id);
    }
    setCompletionNotices((current) => current.filter((notice) => notice.id !== id));
  };

  const pushCompletionNotice = (message: string) => {
    const id = Date.now() + Math.floor(Math.random() * 10_000);
    setCompletionNotices((current) => [...current, { id, message }]);
    const timer = window.setTimeout(() => {
      completionNoticeTimersRef.current.delete(id);
      setCompletionNotices((current) =>
        current.filter((notice) => notice.id !== id)
      );
    }, COMPLETION_NOTICE_MS);
    completionNoticeTimersRef.current.set(id, timer);
  };

  const applyProcessingStatus = (nextStatus: ProcessingStatus) => {
    processingIsAnimatingRef.current = nextStatus.animate;
    setProcessingStatus(nextStatus);
  };

  const clearPendingCompletion = () => {
    if (processingCompletionTimerRef.current !== null) {
      window.clearTimeout(processingCompletionTimerRef.current);
      processingCompletionTimerRef.current = null;
    }
  };

  const showAnimatingStatus = (nextStatus: ProcessingStatus) => {
    clearPendingCompletion();
    if (!processingIsAnimatingRef.current) {
      processingStartedAtRef.current = Date.now();
    }
    applyProcessingStatus({ ...nextStatus, animate: true });
  };

  const showCompletedStatus = (nextStatus: ProcessingStatus) => {
    clearPendingCompletion();
    const startedAt = processingStartedAtRef.current;
    const elapsed =
      startedAt === null ? MIN_PROCESSING_VISUAL_MS : Date.now() - startedAt;
    const remaining = Math.max(0, MIN_PROCESSING_VISUAL_MS - elapsed);
    const finalize = () => {
      processingStartedAtRef.current = null;
      processingCompletionTimerRef.current = null;
      applyProcessingStatus({ ...nextStatus, animate: false });
    };

    if (remaining === 0) {
      finalize();
      return;
    }

    processingCompletionTimerRef.current = window.setTimeout(finalize, remaining);
  };

  useEffect(() => {
    let active = true;

    const progressUnlistenPromise = listen<BatchProgressEvent>(
      "batch-progress",
      (event) => {
        if (!active) {
          return;
        }

        batchProgressRef.current = event.payload;
        setProgressText(`已处理 ${event.payload.processed} / ${event.payload.total}`);
        showAnimatingStatus(toBatchProcessingStatus(event.payload));
      }
    );
    const resultsUnlistenPromise = listen<BatchResultsEvent>(
      "batch-results",
      (event) => {
        if (!active) {
          return;
        }

        setResults(event.payload.rows);
        if (batchStartedAtRef.current !== null) {
          const durationLabel = formatDuration(Date.now() - batchStartedAtRef.current);
          appendLog(
            `批量鉴定完成，用时 ${durationLabel}。`
          );
          pushCompletionNotice(`批量鉴定完成 · 用时 ${durationLabel}`);
          batchStartedAtRef.current = null;
        }
        showCompletedStatus(
          toResultProcessingStatus(event.payload.rows, batchProgressRef.current)
        );
      }
    );
    const buildProgressUnlistenPromise = listen<BuildProgressEvent>(
      "build-progress",
      (event) => {
        if (!active) {
          return;
        }

        const nextBuildStatus = reduceBuildStatus(
          buildStatusRef.current,
          event.payload
        );
        buildStatusRef.current = nextBuildStatus;
        const eventReceivedAt = Date.now();
        if (event.payload.stage === "indexStart") {
          buildIndexStartedAtRef.current = eventReceivedAt;
        }
        if (event.payload.stage === "metaStart") {
          buildMetaStartedAtRef.current = eventReceivedAt;
        }
        if (event.payload.stage === "done") {
          if (buildStartedAtRef.current !== null) {
            const totalDurationMs = eventReceivedAt - buildStartedAtRef.current;
            const detailSegments: string[] = [];
            if (buildIndexStartedAtRef.current !== null) {
              detailSegments.push(
                `流式解码: ~${formatCompactDuration(
                  buildIndexStartedAtRef.current - buildStartedAtRef.current
                )}`
              );
            }
            if (
              buildIndexStartedAtRef.current !== null &&
              buildMetaStartedAtRef.current !== null
            ) {
              detailSegments.push(
                `索引写入: ~${formatCompactDuration(
                  buildMetaStartedAtRef.current - buildIndexStartedAtRef.current
                )}`
              );
            }
            appendLog(
              `✅ 数据库构建完成！总耗时: ${formatCompactDuration(
                totalDurationMs
              )}${detailSegments.length > 0 ? ` (${detailSegments.join(", ")})` : ""}`
            );
            pushCompletionNotice(
              `构建数据库成功 · 用时 ${formatDuration(totalDurationMs)}`
            );
            buildStartedAtRef.current = null;
            buildIndexStartedAtRef.current = null;
            buildMetaStartedAtRef.current = null;
          }
          showCompletedStatus(toBuildProcessingStatus(nextBuildStatus));
          return;
        }

        showAnimatingStatus(toBuildProcessingStatus(nextBuildStatus));
      }
    );

    return () => {
      active = false;
      clearPendingCompletion();
      completionNoticeTimersRef.current.forEach((timer) => {
        window.clearTimeout(timer);
      });
      completionNoticeTimersRef.current.clear();
      void progressUnlistenPromise.then((unlisten) => unlisten());
      void resultsUnlistenPromise.then((unlisten) => unlisten());
      void buildProgressUnlistenPromise.then((unlisten) => unlisten());
    };
  }, []);

  const handleBuildStart = () => {
    setIsBuildingDatabase(true);
    const startedAt = Date.now();
    buildStartedAtRef.current = startedAt;
    buildIndexStartedAtRef.current = null;
    buildMetaStartedAtRef.current = null;
    clearPendingCompletion();
    processingStartedAtRef.current = startedAt;
    const initialBuildStatus: BuildStatus = {
      stage: "start",
      stageLabel: "正在准备构建",
      stageDetail: null,
      genomesProcessed: 0,
      peaksProcessed: 0,
      allDb: null
    };
    buildStatusRef.current = initialBuildStatus;
    applyProcessingStatus({ ...toBuildProcessingStatus(initialBuildStatus), animate: true });
  };

  const handleBuildEnd = () => {
    setIsBuildingDatabase(false);
  };

  const handleRunStart = () => {
    const startedAt = Date.now();
    batchStartedAtRef.current = startedAt;
    clearPendingCompletion();
    processingStartedAtRef.current = startedAt;
    applyProcessingStatus({
      stateLabel: "处理中",
      headline: "正在启动批量鉴定",
      summary: "等待首个进度事件。",
      animate: true
    });
  };

  const handleRunFailure = () => {
    clearPendingCompletion();
    batchStartedAtRef.current = null;
    processingStartedAtRef.current = null;
    applyProcessingStatus(idleProcessingStatus);
  };

  return (
    <main className="workbench-shell">
      <section className="workbench-header">
        <div className="hero-copy">
          <p className="eyebrow">Desktop Workbench</p>
          <h1>GPMsDB Workbench</h1>
        </div>
        <div className="header-metrics" aria-label="当前摘要">
          <span className="metric-pill">{progressText}</span>
          <span className="metric-pill">结果 {results.length} 条</span>
          <span className="metric-pill">{processingStatus.stateLabel}</span>
        </div>
      </section>

      <section className="workbench-layout">
        <div className="layout-column layout-left-column">
          <div className="layout-slot layout-database">
            <DatabasePanel
              onLog={appendLog}
              isBuildingDatabase={isBuildingDatabase}
              onBuildStart={handleBuildStart}
              onBuildEnd={handleBuildEnd}
              onSuccessNotice={pushCompletionNotice}
            />
          </div>

          <div className="layout-slot layout-query">
            <BatchRunPanel
              onLog={appendLog}
              isBuildingDatabase={isBuildingDatabase}
              onRunStart={handleRunStart}
              onRunFailure={handleRunFailure}
              onSuccessNotice={pushCompletionNotice}
              onCancel={() => {
                appendLog("当前版本暂未实现取消功能。");
              }}
            />
          </div>

          <div className="layout-slot layout-log">
            <LogConsole lines={logs} />
          </div>
        </div>

        <div className="layout-column layout-right-column">
          <div className="layout-slot layout-summary">
            <ProcessingStatusPanel
              status={processingStatus}
              resultCount={results.length}
              progressText={progressText}
            />
          </div>

          <div className="layout-slot layout-result">
            <ResultTable rows={results} />
          </div>
        </div>
      </section>

      <CompletionToasts
        notices={completionNotices}
        onDismiss={dismissCompletionNotice}
      />
    </main>
  );
}

export default App;
