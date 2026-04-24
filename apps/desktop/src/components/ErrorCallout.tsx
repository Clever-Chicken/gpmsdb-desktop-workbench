type ErrorCalloutProps = {
  title: string;
  message: string;
  onDismiss?: () => void;
};

export function ErrorCallout({ title, message, onDismiss }: ErrorCalloutProps) {
  return (
    <div className="error-callout" role="alert" aria-live="assertive">
      <svg
        className="error-callout-icon"
        viewBox="0 0 20 20"
        fill="none"
        aria-hidden="true"
      >
        <path
          d="M10 2.5 1.75 17h16.5L10 2.5Z"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinejoin="round"
        />
        <path
          d="M10 8v4"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinecap="round"
        />
        <circle cx="10" cy="14.3" r="0.9" fill="currentColor" />
      </svg>
      <div className="error-callout-body">
        <p className="error-callout-title">{title}</p>
        <p className="error-callout-detail">{message}</p>
      </div>
      {onDismiss ? (
        <button
          type="button"
          className="error-callout-dismiss"
          onClick={onDismiss}
          aria-label="关闭错误提示"
        >
          关闭
        </button>
      ) : null}
    </div>
  );
}
