export type ProcessingStatus = {
  stateLabel: string;
  headline: string;
  summary: string;
  animate: boolean;
};

type ProcessingStatusPanelProps = {
  status: ProcessingStatus;
  resultCount: number;
  progressText: string;
};

export function ProcessingStatusPanel({
  status,
  resultCount,
  progressText
}: ProcessingStatusPanelProps) {
  return (
    <section
      className={`workspace-card processing-status-card workbench-overview-card${status.animate ? " is-processing" : ""}`}
      aria-live="polite"
      data-testid="processing-status-card"
    >
      <div className="overview-heading">
        <div className="section-header section-header-tight">
          <h2>鉴定结果与处理状态</h2>
          <span
            className={`state-pill${status.animate ? " is-active" : " is-static"}`}
          >
            {status.stateLabel}
          </span>
        </div>
        <p className="overview-intro">
          集中显示结果数量、处理进度与当前状态，处理中的彩色轨迹会持续反馈。
        </p>
      </div>

      <div className="overview-grid">
        <div className="overview-panel overview-panel-results">
          <p className="overview-label">鉴定结果</p>
          <p className="overview-metric">结果 {resultCount} 条</p>
          <p className="overview-copy">
            右侧结果表保持固定高度，过长内容在卡片内滚动查看。
          </p>
          <p className="overview-progress">{progressText}</p>
        </div>

        <div className="overview-panel overview-panel-status">
          <p className="overview-label">处理状态</p>
          <p className="status-headline">{status.headline}</p>
          <p className="status-summary">{status.summary}</p>
          <div className="processing-track" aria-hidden="true">
            <div
              className={`processing-bar${status.animate ? " is-animated" : ""}`}
            />
          </div>
        </div>
      </div>
    </section>
  );
}
