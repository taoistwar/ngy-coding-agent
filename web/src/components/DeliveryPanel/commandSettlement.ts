import type { DeliveryErrorState } from "../../state/deliveryModel";
import type { DeliveryPollingController } from "../../state/useDeliveryPolling";

export function refreshAfterSettledRejection(
  controller: DeliveryPollingController,
  taskId: string,
  error: DeliveryErrorState,
): void {
  if (!error.retryable && controller.state.taskId === taskId) {
    controller.refresh();
  }
}
