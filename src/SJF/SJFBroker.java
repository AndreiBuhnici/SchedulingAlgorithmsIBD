package SJF;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.*;
import org.cloudbus.cloudsim.lists.VmList;

import java.util.*;

public class SJFBroker extends DatacenterBroker {
    private Map<Integer, Double> vmAvailableTime;
    private PriorityQueue<GuestEntity> availableVMQueue;

    public SJFBroker(String name) throws Exception {
        super(name);
        vmAvailableTime = new HashMap<>();
        availableVMQueue = new PriorityQueue<>(Comparator.comparingDouble(vm -> vmAvailableTime.get(vm.getId())));
    }

    @Override
    protected void processCloudletReturn(SimEvent ev) {
        super.processCloudletReturn(ev);
        Cloudlet cloudlet = (Cloudlet) ev.getData();
        System.out.println("Cloudlet " + cloudlet.getCloudletId() + " finished execution");
    }

    @Override
    public void submitCloudletList(List<? extends Cloudlet> list) {
        // Sort cloudlets based on their length (execution time) in ascending order
        list.sort(Comparator.comparingLong(Cloudlet::getCloudletLength));
        // Call the original method to process the sorted cloudlets
        super.submitCloudletList(list);
    }

    @Override
    protected void submitCloudlets() {
        if (getCloudletList().isEmpty() || getGuestsCreatedList().isEmpty()) {
            return;
        }

        // Initialize VM availability times and populate the priority queue
        for (GuestEntity vm : getGuestsCreatedList()) {
            vmAvailableTime.put(vm.getId(), 0.0);
            availableVMQueue.add(vm); // Add all VMs to the priority queue
        }

        // Sort cloudlets by length (shortest first)
        List<Cloudlet> sortedCloudlets = new ArrayList<>(getCloudletList());
        sortedCloudlets.sort(Comparator.comparingLong(Cloudlet::getCloudletLength));

        List<Cloudlet> successfullySubmitted = new ArrayList<>();
        for (Cloudlet cloudlet : sortedCloudlets) {
            GuestEntity selectedVm = null;
            double earliestCompletionTime = Double.MAX_VALUE;

            // If VM is specified, use it
            if (cloudlet.getGuestId() != -1) {
                selectedVm = getVmById(cloudlet.getGuestId());
                if (selectedVm != null) {
                    earliestCompletionTime = calculateCompletionTime(cloudlet, selectedVm);
                }
            } else {
                // Get the VM with the earliest available time from the priority queue
                while (!availableVMQueue.isEmpty()) {
                    selectedVm = availableVMQueue.poll();
                    double completionTime = calculateCompletionTime(cloudlet, selectedVm);
                    if (completionTime < earliestCompletionTime) {
                        earliestCompletionTime = completionTime;
                        break;
                    }
                }
            }

            if (selectedVm == null) continue;

            // Submit cloudlet and update VM availability
            cloudlet.setGuestId(selectedVm.getId());
            sendNow(getVmsToDatacentersMap().get(selectedVm.getId()),
                    CloudActionTags.CLOUDLET_SUBMIT, cloudlet);

            // Update VM's available time and reinsert into the priority queue
            vmAvailableTime.put(selectedVm.getId(), earliestCompletionTime);
            availableVMQueue.add(selectedVm); // Re-insert the VM with updated availability

            cloudletsSubmitted++;
            getCloudletSubmittedList().add(cloudlet);
            successfullySubmitted.add(cloudlet);
        }

        getCloudletList().removeAll(successfullySubmitted);
    }

    private double calculateCompletionTime(Cloudlet cloudlet, GuestEntity vm) {
        double processingTime = cloudlet.getCloudletLength() / ((Vm) vm).getMips();
        double availableTime = vmAvailableTime.get(vm.getId());
        return availableTime + processingTime;
    }

    private GuestEntity getVmById(int guestId) {
        GuestEntity vm = VmList.getById(getGuestsCreatedList(), guestId);
        return vm != null ? vm : VmList.getById(getGuestList(), guestId);
    }
}
