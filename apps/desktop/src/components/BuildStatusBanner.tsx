import type { BuildProgressEvent, BuildStage } from "../lib/events";

export type BuildStatus = {
  stage: BuildStage;
  stageLabel: string;
  stageDetail: string | null;
  genomesProcessed: number;
  peaksProcessed: number;
  allDb: string | null;
};

type BuildStatusBannerProps = {
  status: BuildStatus;
};

const formatCount = (value: number) => value.toLocaleString("en-US");

export function BuildStatusBanner({ status }: BuildStatusBannerProps) {
  const isComplete = status.stage === "done";
  const showCounter = status.stage === "genome" || status.stage === "done";
  const showBar = !isComplete;

  return (
    <section
      className="build-status"
      role="status"
      aria-live="polite"
      aria-busy={showBar}
    >
      <div className="build-status-heading">
        <span
          className={`build-status-pulse${isComplete ? " is-complete" : ""}`}
          aria-hidden="true"
        />
        <div className="build-status-text">
          <p className="build-status-eyebrow">
            {isComplete ? "构建完成" : "数据库构建中"}
          </p>
          <p className="build-status-stage">{status.stageLabel}</p>
        </div>
      </div>

      <div className="build-status-meta">
        {showCounter ? (
          <p className="build-status-detail">
            已处理{" "}
            <span className="metric">{formatCount(status.genomesProcessed)}</span>{" "}
            个基因组
            <span className="sep" aria-hidden="true">
              ·
            </span>
            <span className="metric">{formatCount(status.peaksProcessed)}</span>{" "}
            个峰值
          </p>
        ) : null}
        {status.stageDetail ? (
          <p className="build-status-sub">
            <span className="build-status-sub-label">
              {status.stage === "genome" ? "当前基因组" : "数据源"}
            </span>
            <code className="build-status-sub-value">{status.stageDetail}</code>
          </p>
        ) : null}
      </div>

      {showBar ? (
        <div className="build-status-track" aria-hidden="true">
          <div className="build-status-bar" />
        </div>
      ) : null}
    </section>
  );
}

export function reduceBuildStatus(
  prev: BuildStatus | null,
  event: BuildProgressEvent
): BuildStatus {
  const base: BuildStatus =
    prev ?? {
      stage: "start",
      stageLabel: "正在准备构建",
      stageDetail: null,
      genomesProcessed: 0,
      peaksProcessed: 0,
      allDb: null
    };

  switch (event.stage) {
    case "start":
      return {
        ...base,
        stage: "start",
        stageLabel: "正在准备构建",
        stageDetail: event.allDb,
        allDb: event.allDb,
        genomesProcessed: 0,
        peaksProcessed: 0
      };
    case "genome":
      return {
        ...base,
        stage: "genome",
        stageLabel: "正在流式解码",
        stageDetail: event.accession,
        genomesProcessed: event.genomesProcessed,
        peaksProcessed: event.peaksProcessed
      };
    case "indexStart":
      return {
        ...base,
        stage: "indexStart",
        stageLabel: "正在写入 mass_index.bin",
        stageDetail: null
      };
    case "metaStart":
      return {
        ...base,
        stage: "metaStart",
        stageLabel: "正在写入 meta.bin",
        stageDetail: null
      };
    case "finalizeStart":
      return {
        ...base,
        stage: "finalizeStart",
        stageLabel: "正在计算 CRC 并收尾",
        stageDetail: null
      };
    case "done":
      return {
        ...base,
        stage: "done",
        stageLabel: "构建完成",
        stageDetail: null,
        genomesProcessed: event.genomeCount,
        peaksProcessed: event.totalPeakCount
      };
  }
}
