package Metrics;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import utils.Constants;
import utils.DatacenterCreator;
import utils.GenerateMatrices;

import java.text.DecimalFormat;
import java.util.*;

public class Scheduler {

    private static List<Cloudlet> cloudletList;
    private static List<Vm> vmList;
    private static Datacenter[] datacenter;
    private static double[][] commMatrix;
    private static double[][] execMatrix;
    private static String algName = "";
    private static Map<String, Map<String, ArrayList<Double>>> metrics = new HashMap<>();
    private static List<String> algNames = Arrays.asList("RASA", "FCFS", "RR", "SJF", "MINMIN");

    // Old init:true
    // Balanced: true
    // {RR={Throughput=[0.001679230554367414, 0.002380455591205991, 0.0032764349389270827, 0.003980139276950016], Makespan=[2977554.2059997586, 2100438.260000007, 1526048.920000018, 1256237.4460000065], Fairness=[0.8811750702839969, 0.8820646511523038, 0.8864922896809935, 0.88590682552284], Average delay=[4144.31582879968, 4262.030453600017, 4204.4356336000465, 4206.702401600035]}, FCFS={Throughput=[], Makespan=[], Fairness=[], Average delay=[]}, RASA={Throughput=[0.001677119651436385, 0.002399055618089434, 0.0033693851706322942, 0.003949041474041948], Makespan=[2981301.897999766, 2084153.4320000103, 1483950.2600000186, 1266130.030000006], Fairness=[0.8870937598446657, 0.8788157932983098, 0.8818748135786986, 0.8829861893397831], Average delay=[4177.646107999671, 4205.81985280004, 4153.9185768000525, 4212.952909600045]}, SJF={Throughput=[0.0016434868654637046, 0.002459989316797738, 0.0032596718937356753, 0.0038650911934486093], Makespan=[3042312.113999929, 2032529.152000014, 1533896.7120000105, 1293630.5380000034], Fairness=[0.8656772753224735, 0.8667889876811217, 0.8702288881720519, 0.8725170830591785], Average delay=[4641.287321599901, 4630.289892800022, 4617.8961024000355, 4700.200204800021]}, MINMIN={Throughput=[0.0016946371579608093, 0.0023907229240109564, 0.0033772645291534, 0.003919780820030459], Makespan=[2950484.105999776, 2091417.6000000096, 1480488.1160000165, 1275581.526000004], Fairness=[0.8862924774810849, 0.8834480498499626, 0.8851274567199925, 0.8840088709391336], Average delay=[4125.978015999696, 4208.685295200048, 4142.090668000043, 4227.449345600031]}}

    // Old init:true
    //Balanced: false
    //{RR={Throughput=[0.0023412792545874387, 0.0033996444869372486, 0.004667570529875196, 0.005517065527222222], Makespan=[2135584.634000039, 1470742.020000014, 1071221.0920000158, 906278.8860000075], Fairness=[0.8069273889426162, 0.8051235668857063, 0.8007955232512773, 0.8011016858993395], Average delay=[2785.2421144000505, 2808.859417600046, 2760.467615200047, 2857.509789600032]}, FCFS={Throughput=[], Makespan=[], Fairness=[], Average delay=[]}, RASA={Throughput=[0.0023880834004044523, 0.0034560489881727916, 0.004787060372076913, 0.005579904809368352], Makespan=[2093729.22200003, 1446738.7520000094, 1044482.3360000161, 896072.6340000059], Fairness=[0.8023407421531226, 0.8026548195186728, 0.8045956743537033, 0.8032366078077627], Average delay=[2802.222328000047, 2779.539428800037, 2796.1274440000384, 2852.4677240000424]}, SJF={Throughput=[0.0024316039676108158, 0.0035361527201557535, 0.004519793998492392, 0.005646019530823649], Makespan=[2056255.8980000245, 1413966.0800000092, 1106245.108000007, 885579.650000005], Fairness=[0.7634088637753658, 0.7621607918905651, 0.7685180225365883, 0.7666221980010552], Average delay=[3260.9225432000367, 3252.344039200019, 3276.159175200017, 3277.2486312000233]}, MINMIN={Throughput=[0.0024265106664123535, 0.003418358810110841, 0.004794998305811949, 0.005564511537951205], Makespan=[2060572.0260000357, 1462690.2200000104, 1042753.2360000153, 898551.4660000055], Fairness=[0.8014768223388584, 0.8056339498837294, 0.7999219072861873, 0.8021114000795614], Average delay=[2754.247689600052, 2816.8881824000373, 2793.9756112000405, 2852.996246400042]}}

    public static void main(String[] args) {
        initMetrics();

        try {
            // uniform lengths or majority small (unbalanced)
            Constants.UNBALANCED = true;
            Constants.NO_OF_TASKS = 5000;
            Constants.USE_OLD_MATRIX_INIT = true;

            for (int n_datacenters=4; n_datacenters<=10; n_datacenters+=2){
                Constants.NO_OF_DATA_CENTERS = n_datacenters;

                if (Constants.USE_OLD_MATRIX_INIT) {
                    new GenerateMatrices();
                    execMatrix = GenerateMatrices.getExecMatrix();
                    commMatrix = GenerateMatrices.getCommMatrix();
                } else {
                    initTaskLengths();
                }

                for (var alg : algNames){
                    runBroker(n_datacenters, alg);
                }
//                runBroker(n_datacenters, "FCFS");
//                runBroker(n_datacenters, "RASA");
//                runBroker(n_datacenters, "MINMIN");
//                runBroker(n_datacenters, "RR");
//                runBroker(n_datacenters, "SJF");
            }

            System.out.println("Old init:"+Constants.USE_OLD_MATRIX_INIT);
            System.out.println("Balanced: "+!Constants.UNBALANCED);
            System.out.println(metrics);

            print("{");
            for (String metric : Arrays.asList("Makespan", "Average_delay", "Throughput", "Fairness")){
                printQuotes(metric);
                print(":{");
                print("\n");
                for (String alg : algNames) {
                    printQuotes(alg);
                    print(":");
                    System.out.print(metrics.get(alg).get(metric));
                    print(",");
                    print("\n");
                }
                print("},");
                print("\n");
            }
            print("}");

        } catch (Exception e) {
            e.printStackTrace();
            Log.println("The simulation has been terminated due to an unexpected error");
        }
    }

    private static void printQuotes(String s){
        System.out.print("\""+s+"\"");
    }
    private static void print(String s){
        System.out.print(s);
    }

    private static void initTaskLengths() {
        var zipf = new ZipfDistribution(6, 1);

        execMatrix = new double[Constants.NO_OF_TASKS][Constants.NO_OF_DATA_CENTERS];
        for (int i = 0; i < Constants.NO_OF_TASKS; i++) {
            if (!Constants.UNBALANCED) {
                execMatrix[i][0] = Math.random() * 600 + 10;
            } else {
                execMatrix[i][0] = (zipf.sample() * 100) + Math.random() * 20;
            }
        }
    }

    private static List<Vm> createVM(int userId, int vms) {
        //Creates a container to store VMs. This list is passed to the broker later
        LinkedList<Vm> list = new LinkedList<>();

        //VM description
        int mips = 250;
        long size = 10000; //image size (MB)
        int ram = 512; //vm memory (MB)
        long bw = 1000;
        int pesNumber = 1; //number of cpus
        String vmm = "Xen"; //VMM name

        //create VMs
        Vm[] vm = new Vm[vms];

        for (int i = 0; i < vms; i++) {
            // datacenter.getId() starts from 2, 0 and 1 are not available
            vm[i] = new Vm(datacenter[i].getId(), userId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
            list.add(vm[i]);
        }

        return list;
    }

    private static List<Cloudlet> createCloudlet(int brokerId, int cloudlets, int idShift) {
        // Creates a container to store Cloudlets
        LinkedList<Cloudlet> list = new LinkedList<Cloudlet>();

        //cloudlet parameters
        long fileSize = 300;
        long outputSize = 300;
        int pesNumber = 1;
        UtilizationModel utilizationModel = new UtilizationModelFull();

        Cloudlet[] cloudlet = new Cloudlet[cloudlets];

        for (int i = 0; i < cloudlets; i++) {
            int dcId = 0;
            if (Constants.USE_OLD_MATRIX_INIT) {
                dcId = (int) (Math.random() * Constants.NO_OF_DATA_CENTERS);
            }
            // 1000 is needed or simulation units and comm + exec time is the time needed for a task
            long length = 0;
            if (Constants.USE_OLD_MATRIX_INIT) {
                length = (long) (1e3 * (commMatrix[i][dcId] + execMatrix[i][dcId]));
            } else {
                length = (long) ((execMatrix[i][dcId]));
            }

            cloudlet[i] = new Cloudlet(idShift + i, length, pesNumber, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
            // setting the owner of these Cloudlets
            cloudlet[i].setUserId(brokerId);
            // bind cloudlet to certain vms
            // datacenter.getId() starts from 2, 0 and 1 are not available
            cloudlet[i].setGuestId(dcId + 2);
            list.add(cloudlet[i]);
        }
        return list;
    }

    private static DatacenterBroker createBroker(String name, int n_broker) {
        DatacenterBroker broker = null;
        try {
            if (name.equalsIgnoreCase("RR")){
                broker = new RRBroker(name+"_"+n_broker);
            } else if (name.equalsIgnoreCase("FCFS")){
                broker = new FCFSBroker(name+"_"+n_broker);
            } else if (name.equalsIgnoreCase("RASA")) {
                broker = new RASABroker(name+"_"+n_broker);
            }  else if (name.equalsIgnoreCase("SJF")) {
                broker = new SJFBroker(name+"_"+n_broker);
            }  else if (name.equalsIgnoreCase("MINMIN")) {
                broker = new MinMinBroker(name+"_"+n_broker);
            }
    } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return broker;
    }

    public static void runBroker(int n_broker, String algName) {
        try {

            int num_user = 1;   // number of grid users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;  // mean trace events

            CloudSim.init(num_user, calendar, trace_flag);

            // Second step: Create Datacenters
            datacenter = new Datacenter[Constants.NO_OF_DATA_CENTERS];
            for (int i = 0; i < Constants.NO_OF_DATA_CENTERS; i++) {
                datacenter[i] = DatacenterCreator.createDatacenter("Datacenter_" + i);
                System.out.println(datacenter[i].getId());
            }

            DatacenterBroker broker = createBroker(algName, n_broker);
            int brokerId = broker.getId();

            //Fourth step: Create VMs and Cloudlets and send them to broker
            vmList = createVM(brokerId, Constants.NO_OF_DATA_CENTERS);
            cloudletList = createCloudlet(brokerId, Constants.NO_OF_TASKS, 0);

            broker.submitGuestList(vmList);
            broker.submitCloudletList(cloudletList);

            // Fifth step: Starts the simulation
            CloudSim.startSimulation();

            // Final step: Print results when simulation is over
            List<Cloudlet> newList = broker.getCloudletReceivedList();

            CloudSim.stopSimulation();

            printCloudletList(newList, algName);

            Log.println(Scheduler.class.getName() + " finished!");
        } catch (Exception e) {
            e.printStackTrace();
            Log.println("The simulation has been terminated due to an unexpected error");
        }
    }

    public static void initMetrics() {
        for (String alg : algNames){
            Map<String, ArrayList<Double>> mets = new HashMap<>(Map.ofEntries(
                    Map.entry("Makespan", new ArrayList<Double>()),
                    Map.entry("Average_delay", new ArrayList<Double>()),
                    Map.entry("Throughput", new ArrayList<Double>()),
                    Map.entry("Fairness", new ArrayList<Double>())
            ));
            metrics.put(alg, mets);
        }
    }

    /**
     * Prints the Cloudlet objects
     *
     * @param list list of Cloudlets
     */
    private static void printCloudletList(List<Cloudlet> list, String algName) {
        int size = list.size();
        Cloudlet cloudlet;

        String indent = "    ";
        Log.println();
        Log.println("========== OUTPUT ==========");
        Log.println("Cloudlet ID" + indent + "STATUS" + indent
                + "Data center ID" + indent + "VM ID" + indent + "Time" + indent
                + "Start Time" + indent + "Finish Time");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (Cloudlet value : list) {
            cloudlet = value;
            Log.print(indent + cloudlet.getCloudletId() + indent + indent);

            if (cloudlet.getStatus() == Cloudlet.CloudletStatus.SUCCESS) {
                Log.print("SUCCESS");

                Log.println(indent + indent + cloudlet.getResourceId()
                        + indent + indent + indent + cloudlet.getGuestId()
                        + indent + indent
                        + dft.format(cloudlet.getActualCPUTime()) + indent
                        + indent + dft.format(cloudlet.getExecStartTime())
                        + indent + indent
                        + dft.format(cloudlet.getExecFinishTime()));
            }
        }
        double makespan = calcMakespan(list);
        Log.println("Makespan using "+algName+": " + makespan);
        double averageDelay = calculateAverageDelay(list);
        Log.println("Average delay using "+algName+": " + averageDelay);
        double throughput = calculateThroughput(list);
        Log.println("Throughput using "+algName+": " + throughput);
        double fairness = calculateFairness(list);
        Log.println("Fairness using "+algName+": " + fairness);

        var algMetrics = metrics.get(algName);
        algMetrics.get("Makespan").add(makespan);
        algMetrics.get("Average_delay").add(averageDelay);
        algMetrics.get("Throughput").add(throughput);
        algMetrics.get("Fairness").add(fairness);
    }

    /*
    *
Makespan using RASA: 1375183.708000003
Average delay using RASA: 3693.737204800035
Throughput using RASA: 0.0018179389309635383
Fairness using RASA: 0.7685382623886272

Makespan using FCFS: 1437547.3760000044
Average delay using FCFS: 3656.697529600039
Throughput using FCFS: 0.0017390731197717358
Fairness using FCFS: 0.7647815229273934

Makespan using RASA: 817.6599999999997
Average delay using RASA: 2.3279159999999637
Throughput using RASA: 3.6690066775921544
Fairness using RASA: 0.7490471148214973
    *
    *  */

    private static double calcMakespan(List<Cloudlet> list) {
        double makespan = 0;

        for (Cloudlet cloudlet : list) {
            // Get the finish time of the cloudlet
            double finishTime = cloudlet.getExecFinishTime();
            // Update the makespan to be the maximum finish time
            makespan = Math.max(makespan, finishTime);
        }

        return makespan;
    }

    private static double calculateAverageDelay(List<Cloudlet> tasks) {
        double totalDelay = 0.0;
        for (Cloudlet task : tasks) {
            totalDelay += task.getExecFinishTime() - task.getExecStartTime();
        }
        return totalDelay / tasks.size();
    }

    private static double calculateThroughput(List<Cloudlet> list) {
        double totalTime = calcMakespan(list);
        return list.size() / totalTime;
    }

    private static double calculateFairness(List<Cloudlet> list) {
        double sum = 0.0, sumSq = 0.0;
        for (Cloudlet task : list) {
            double time = task.getActualCPUTime();
            sum += time;
            sumSq += time * time;
        }
        int n = list.size();
        return (sum * sum) / (n * sumSq);
    }

}