package SJF;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.*;
import org.cloudbus.cloudsim.lists.VmList;

import java.util.*;

public class SJFBroker extends DatacenterBroker {
    // Add our own index tracker since we can't access the parent's private guestIndex
    private int currentVmIndex;

    public SJFBroker(String name) throws Exception {
        super(name);
        currentVmIndex = 0;
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
        // Validate if there are cloudlets and VMs available
        if (getCloudletList().isEmpty() || getGuestsCreatedList().isEmpty()) {
            return;
        }

        // Sort cloudlets by length (shortest first)
        List<Cloudlet> cloudletList = new ArrayList<>(getCloudletList());
        cloudletList.sort(Comparator.comparingLong(Cloudlet::getCloudletLength));

        // Process each cloudlet in sorted order
        List<Cloudlet> successfullySubmitted = new ArrayList<>();
        for (Cloudlet cloudlet : cloudletList) {
            // Get the VM for the cloudlet, either by guestId or by round-robin index
            GuestEntity vm = (cloudlet.getGuestId() == -1)
                    ? getGuestsCreatedList().get(currentVmIndex)
                    : getVmById(cloudlet.getGuestId());

            // If VM is still not found, skip this cloudlet
            if (vm == null) {
                logCloudletPostponed(cloudlet);
                continue;
            }

            // Log submission if logging is enabled
            logCloudletSubmission(cloudlet, vm);

            // Submit the cloudlet to the VM
            cloudlet.setGuestId(vm.getId());
            sendNow(getVmsToDatacentersMap().get(vm.getId()),
                    CloudActionTags.CLOUDLET_SUBMIT, cloudlet);

            cloudletsSubmitted++;
            getCloudletSubmittedList().add(cloudlet);
            successfullySubmitted.add(cloudlet);

            // Update round-robin index
            currentVmIndex = (currentVmIndex + 1) % getGuestsCreatedList().size();
        }

        // Remove submitted cloudlets from the waiting list
        getCloudletList().removeAll(successfullySubmitted);
    }

    private GuestEntity getVmById(int guestId) {
        // First try to find VM in created list
        GuestEntity vm = VmList.getById(getGuestsCreatedList(), guestId);
        if (vm == null) {
            // If not found in created list, try in the main list
            vm = VmList.getById(getGuestList(), guestId);
        }
        return vm;
    }

    private void logCloudletPostponed(Cloudlet cloudlet) {
        if (!Log.isDisabled()) {
            Log.printlnConcat(new Object[]{
                    CloudSim.clock(), ": ", getName(),
                    ": Postponing execution of cloudlet ", cloudlet.getCloudletId(),
                    ": bound guest entity of id ", cloudlet.getGuestId(), " doesn't exist"
            });
        }
    }

    private void logCloudletSubmission(Cloudlet cloudlet, GuestEntity vm) {
        if (!Log.isDisabled()) {
            Log.printlnConcat(new Object[]{
                    CloudSim.clock(), ": ", getName(),
                    ": Sending ", cloudlet.getClass().getSimpleName(),
                    " #", cloudlet.getCloudletId(),
                    " to " + vm.getClassName() + " #", vm.getId()
            });
        }
    }
}
