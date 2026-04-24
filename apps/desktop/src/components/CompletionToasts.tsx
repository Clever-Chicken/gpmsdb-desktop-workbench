export type CompletionNotice = {
  id: number;
  message: string;
};

type CompletionToastsProps = {
  notices: CompletionNotice[];
  onDismiss: (id: number) => void;
};

export function CompletionToasts({
  notices,
  onDismiss
}: CompletionToastsProps) {
  if (notices.length === 0) {
    return null;
  }

  const notice = notices[notices.length - 1];

  return (
    <div className="completion-popup-layer" aria-live="polite" aria-atomic="true">
      <section key={notice.id} className="completion-popup" role="status">
        <div className="completion-popup-mark" aria-hidden="true">
          ✓
        </div>
        <div className="completion-popup-body">
          <p className="completion-popup-title">操作已完成</p>
          <p className="completion-popup-copy">{notice.message}</p>
        </div>
        <button
          type="button"
          className="completion-popup-dismiss"
          aria-label="关闭完成提示"
          onClick={() => onDismiss(notice.id)}
        >
          关闭
        </button>
      </section>
    </div>
  );
}
