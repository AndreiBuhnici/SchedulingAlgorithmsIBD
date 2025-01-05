package FCFS;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.core.SimEvent;

import java.util.Comparator;
import java.util.List;

public class FCFSBroker extends DatacenterBroker {
    public FCFSBroker(String name) throws Exception {
        super(name);
    }

    // Task scheduler
    @Override
    public void submitCloudletList(List<? extends Cloudlet> list) {
        list.sort(Comparator.comparingDouble(Cloudlet::getSubmissionTime));
        super.submitCloudletList(list);
    }

    @Override
    protected void processCloudletReturn(SimEvent ev) {
        super.processCloudletReturn(ev);
        Cloudlet cloudlet = (Cloudlet) ev.getData();
        System.out.println("Cloudlet " + cloudlet.getCloudletId() + " finished execution.");
    }

}
