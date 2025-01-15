package FCFS;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.SimEvent;

import java.util.List;

public class FCFSBroker extends DatacenterBroker {
    public FCFSBroker(String name) throws Exception {
        super(name);
    }

    @Override
    public void submitCloudletList(List<? extends Cloudlet> list) {
        List<Vm> vmlist = getGuestList();
        for (int i = 0; i < list.size(); i++) {
            Cloudlet cloudlet = list.get(i);
            Vm vm = vmlist.get(i % vmlist.size());
            cloudlet.setGuestId(vm.getId());
        }

        super.submitCloudletList(list);
    }


    @Override
    protected void processCloudletReturn(SimEvent ev) {
        super.processCloudletReturn(ev);
        Cloudlet cloudlet = (Cloudlet) ev.getData();
        System.out.println("Cloudlet " + cloudlet.getCloudletId() + " finished execution.");
    }

}
