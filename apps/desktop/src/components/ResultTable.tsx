import type { BatchResultRow } from "../lib/events";

type ResultTableProps = {
  rows: BatchResultRow[];
};

export function ResultTable({ rows }: ResultTableProps) {
  return (
    <section
      className="workspace-card result-card result-card-fixed"
      data-testid="result-table-card"
    >
      <div className="section-header">
        <h2>结果表</h2>
        <p>长文件名会被截断显示，完整文本可通过悬停查看。</p>
      </div>
      <div className="result-table-shell">
        <div className="result-table-scroll" data-testid="result-table-scroll">
          <table className="result-table">
            <thead>
              <tr>
                <th>查询文件</th>
                <th>基因组 ID</th>
                <th>得分</th>
                <th>匹配峰数</th>
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 ? (
                <tr>
                  <td colSpan={4}>暂无结果。</td>
                </tr>
              ) : (
                rows.map((row, index) => (
                  <tr key={`${row.queryFile}-${row.genomeId}-${index}`}>
                    <td title={row.queryFile}>{row.queryFile}</td>
                    <td title={String(row.genomeId)}>{row.genomeId}</td>
                    <td>{row.score.toFixed(2)}</td>
                    <td>
                      {row.matchedRibosomal} / {row.matchedTotal}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}
