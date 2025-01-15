package Metrics;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.GuestEntity;
import org.cloudbus.cloudsim.core.SimEvent;

import java.util.*;

public class RASABroker extends DatacenterBroker {
    private final Map<Integer, Double> vmAvailableTime;
    private final PriorityQueue<GuestEntity> vmQueue;

    public RASABroker(String name) throws Exception {
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
        // Initialize VM availability times and populate the priority queue
        for (GuestEntity vm : getGuestList()) {
            vmAvailableTime.put(vm.getId(), 0.0);
            vmQueue.add(vm); // Add all VMs to the priority queue
        }

        // Sort cloudlets by length (shortest first)
        ArrayList<Cloudlet> sortedCloudlets = new ArrayList<>(list);
        sortedCloudlets.sort(Comparator.comparingLong(Cloudlet::getCloudletLength));

        int min_i = 0;
        int max_i = list.size()-1;
        boolean is_min = true;

        for (int i=0; i<list.size(); i++) {
            Cloudlet cloudlet;
            if (is_min) {
                cloudlet = sortedCloudlets.get(min_i);
                min_i += 1;
            } else {
                cloudlet = sortedCloudlets.get(max_i);
                max_i -= 1;
            }
            is_min = !is_min;

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
