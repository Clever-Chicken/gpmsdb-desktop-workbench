import { useEffect, useRef } from "react";

type LogConsoleProps = {
  lines: string[];
};

export function LogConsole({ lines }: LogConsoleProps) {
  const logRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const element = logRef.current;
    if (!element) {
      return;
    }

    element.scrollTop = element.scrollHeight;
  }, [lines]);

  return (
    <section className="sidebar-card log-card">
      <h2>运行日志</h2>
      <div
        ref={logRef}
        className="log-console"
        role="log"
        aria-label="运行日志"
        data-testid="log-console"
      >
        {lines.length === 0 ? <p className="log-empty">暂无运行日志。</p> : null}
        {lines.map((line, index) => (
          <p key={`${index}-${line}`}>{line}</p>
        ))}
      </div>
    </section>
  );
}
