package Metrics;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.GuestEntity;
import org.cloudbus.cloudsim.core.SimEvent;

import java.util.*;

public class MinMinBroker extends DatacenterBroker {
    private final Map<Integer, Double> vmAvailableTime;
    private final PriorityQueue<GuestEntity> vmQueue;

    public MinMinBroker(String name) throws Exception {
        super(name);
        vmAvailableTime = new HashMap<>();
        vmQueue = new PriorityQueue<>(Comparator.comparingDouble(vm -> vmAvailableTime.get(vm.getId())));
    }

    @Override
    protected void processCloudletReturn(SimEvent ev) {
        super.processCloudletReturn(ev);
        Cloudlet cloudlet = (Cloudlet) ev.getData();
        System.out.println("Cloudlet " + cloudlet.getCloudletId() + " finished execution");
    }


    @Override
    public void submitCloudletList(List<? extends Cloudlet> list) {
        for (GuestEntity vm : getGuestList()) {
            vmAvailableTime.put(vm.getId(), 0.0);
            vmQueue.add(vm);
        }

        ArrayList<Cloudlet> sortedCloudlets = new ArrayList<>(list);
        sortedCloudlets.sort(Comparator.comparingLong(Cloudlet::getCloudletLength));

        for (int i = 0; i < list.size(); i++) {
            Cloudlet cloudlet = sortedCloudlets.get(i);

            // Get the VM with the earliest available time from the priority queue
            GuestEntity selectedVm = vmQueue.poll();
            assert selectedVm != null;
            double earliestCompletionTime = calculateCompletionTime(cloudlet, selectedVm);

            // Submit to vm
            cloudlet.setGuestId(selectedVm.getId());

            // Update VM's available time and reinsert into the priority queue
            vmAvailableTime.put(selectedVm.getId(), earliestCompletionTime);
            vmQueue.add(selectedVm); // Re-insert the VM with updated availability
        }
        super.submitCloudletList(list);
    }

    private double calculateCompletionTime(Cloudlet cloudlet, GuestEntity vm) {
        double processingTime = cloudlet.getCloudletLength() / ((Vm) vm).getMips();
        double availableTime = vmAvailableTime.get(vm.getId());
        return availableTime + processingTime;
    }
}
