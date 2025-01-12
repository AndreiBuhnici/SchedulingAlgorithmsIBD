package RR;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.SimEvent;

import java.util.List;

public class RRBroker extends DatacenterBroker {
    private int lastVmIndex;

    public RRBroker(String name) throws Exception {
        super(name);
        lastVmIndex = -1;
    }

    @Override
    public void submitCloudletList(List<? extends Cloudlet> list) {
        List<Vm> vmList = getGuestList();

        for (Cloudlet cloudlet : list) {
            lastVmIndex = (lastVmIndex + 1) % vmList.size();
            Vm vm = vmList.get(lastVmIndex);
            cloudlet.setGuestId(vm.getId());
        }

        super.submitCloudletList(list);
    }

    @Override
    protected void processCloudletReturn(SimEvent ev) {
        super.processCloudletReturn(ev);
        Cloudlet cloudlet = (Cloudlet) ev.getData();
        System.out.println("Cloudlet " + cloudlet.getCloudletId() + " finished execution");
    }
}