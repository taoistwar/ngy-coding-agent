import {
  useId,
  useLayoutEffect,
  useRef,
  type KeyboardEvent,
  type ReactNode,
} from "react";
import { createPortal } from "react-dom";

export interface AccessibleDeliveryDialogProps {
  title: string;
  description: string;
  className?: string;
  busy?: boolean;
  closeLabel?: string;
  onClose(): void;
  children: ReactNode;
  actions: ReactNode;
}

export function restoreDeliveryDialogFocus(
  returnFocus: HTMLElement | null,
  fallback: HTMLElement | null,
): void {
  queueMicrotask(() => {
    if (
      returnFocus !== null &&
      returnFocus.isConnected &&
      !returnFocus.matches(":disabled, [aria-disabled='true']")
    ) {
      returnFocus.focus();
      if (document.activeElement === returnFocus) return;
    }
    fallback?.focus();
  });
}

export function AccessibleDeliveryDialog({
  title,
  description,
  className = "",
  busy = false,
  closeLabel = "Close",
  onClose,
  children,
  actions,
}: AccessibleDeliveryDialogProps) {
  const headingId = useId();
  const descriptionId = useId();
  const dialogRef = useRef<HTMLElement>(null);
  const closeRef = useRef<HTMLButtonElement>(null);

  useLayoutEffect(() => {
    const background =
      document.querySelector<HTMLElement>(".app-shell") ??
      document.querySelector<HTMLElement>(".delivery-panel");
    const wasInert = background?.inert ?? false;
    const hadInertAttribute = background?.hasAttribute("inert") ?? false;
    if (background !== null) {
      background.inert = true;
      background.setAttribute("inert", "");
    }
    closeRef.current?.focus();
    return () => {
      if (background !== null) {
        background.inert = wasInert;
        if (!hadInertAttribute) background.removeAttribute("inert");
      }
    };
  }, []);

  const trapFocus = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.key === "Escape") {
      event.preventDefault();
      onClose();
      return;
    }
    if (event.key !== "Tab") return;
    const focusable = Array.from(
      dialogRef.current?.querySelectorAll<HTMLElement>(
        'button:not(:disabled), [href], input:not(:disabled), select:not(:disabled), textarea:not(:disabled), [tabindex]:not([tabindex="-1"])',
      ) ?? [],
    ).filter((element) => !element.hasAttribute("hidden"));
    if (focusable.length === 0) {
      event.preventDefault();
      dialogRef.current?.focus();
      return;
    }
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (
      (event.shiftKey && document.activeElement === first) ||
      (!event.shiftKey && document.activeElement === last) ||
      !dialogRef.current?.contains(document.activeElement)
    ) {
      event.preventDefault();
      (event.shiftKey ? last : first)?.focus();
    }
  };

  return createPortal(
    <div className="modal-backdrop" onKeyDown={trapFocus}>
      <section
        ref={dialogRef}
        className={`delivery-dialog ${className}`.trim()}
        role="dialog"
        aria-modal="true"
        aria-labelledby={headingId}
        aria-describedby={descriptionId}
        aria-busy={busy || undefined}
        tabIndex={-1}
      >
        <h2 id={headingId}>{title}</h2>
        <p id={descriptionId}>{description}</p>
        {children}
        <div className="dialog-actions">
          <button ref={closeRef} type="button" onClick={onClose}>
            {closeLabel}
          </button>
          {actions}
        </div>
      </section>
    </div>,
    document.body,
  );
}
