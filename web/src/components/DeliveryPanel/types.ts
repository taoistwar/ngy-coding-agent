import type {
  DeliveryClient,
  DeliveryCommand,
  NewDeliveryDeleteBranch,
  NewDeliveryMerge,
  NewDeliveryPreflight,
  NewDeliveryRemoveWorktree,
} from "../../api/deliveryClient";
import type { DeliveryPollingController } from "../../state/useDeliveryPolling";

export interface DeliveryPanelApi {
  newPreflight(taskId: string, input: NewDeliveryPreflight): DeliveryCommand;
  newMerge(taskId: string, input: NewDeliveryMerge): DeliveryCommand;
  newRemoveWorktree(
    taskId: string,
    input: NewDeliveryRemoveWorktree,
  ): DeliveryCommand;
  newDeleteBranch(
    taskId: string,
    input: NewDeliveryDeleteBranch,
  ): DeliveryCommand;
}

export interface DeliveryPanelBinding {
  api: DeliveryPanelApi | DeliveryClient;
  controller: DeliveryPollingController;
}

export interface DeliveryPanelProps extends DeliveryPanelBinding {
  taskId: string;
}
