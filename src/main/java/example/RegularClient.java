package example;

import commonmodels.Request;
import req.FSPropagator;
import req.RequestService;
import req.RequestThread;
import req.gen.ClientRequestGenerator;
import req.gen.RequestGenerator;
import req.gen.SequentialRequestGenerator;
import req.gen.SmartRequestGenerator;
import util.Config;

import java.io.*;

public class RegularClient implements RequestThread.RequestGenerateThreadCallBack {

    public static void main(String[] args) {
        System.out.println(System.getProperty("sun.java.command"));

        RegularClient regularClient = new RegularClient();

        try {
            if (args.length == 0) {
                System.out.println("Usage: RegularClient -r <filename> [number of requests]\n" +
                        "Usage: RegularClient -f <file in> <file out> [number of requests]\n" +
                        "Usage: RegularClient -s <filename>\n");
            } else if (args[0].equals("-r")) {
                regularClient.launchRequestGenerator(args);
            } else if (args[0].equals("-m")) {
                regularClient.launchSmartRequestGenerator(args);
            } else if (args[0].equals("-f")) {
                regularClient.launchFileRequestGenerator(args);
            } else if (args[0].equals("-s")) {
                regularClient.launchSequentialRequestGenerator(args);
            } else if (args[0].equals("-p")) {
                regularClient.launchFilePropagator(args);
            } else {
                System.out.println("Usage: RegularClient -r <filename> [number of requests]\n" +
                        "Usage: RegularClient -f <file in> <file out> [number of requests]\n" +
                        "Usage: RegularClient -s <filename>\n");
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void launchRequestGenerator(String[] args) throws IOException {
        if (args.length >= 2) {
            RequestGenerator generator = new ClientRequestGenerator(args[1], args.length >= 3 ? args[2] : null);
            int numOfRequests = Config.getInstance().getNumberOfRequests();
            if (args.length >= 3) numOfRequests = Integer.parseInt(args[2]);
            generateRequest(generator, numOfRequests);
        }
        else {
            System.out.println ("Usage: RegularClient -r <filename> [rank filename]");
        }
    }

    private void launchSmartRequestGenerator(String[] args) throws IOException {
        if (args.length >= 2) {
            RequestGenerator generator = new SmartRequestGenerator(args[1],
                    args.length >= 3 ? args[2] : null,
                    args.length >= 4 ? args[3] : null);
            int numOfRequests = Config.getInstance().getNumberOfRequests();
            generateRequest(generator, numOfRequests);
        }
        else {
            System.out.println ("Usage: RegularClient -m <static tree file> [dynamic tree file]");
        }
    }

    private void launchFileRequestGenerator(String[] args) {
        if (args.length >= 3) {
            int numOfRequests = Config.getInstance().getNumberOfRequests();
            if (args.length >= 4) numOfRequests = Integer.parseInt(args[3]);
            generateRequestFile(args[1], args[2], numOfRequests);
        }
        else {
            System.out.println ("Usage: RegularClient -f <file in> <file out> [number of requests]");
        }
    }

    private void launchSequentialRequestGenerator(String[] args) throws IOException {
        if (args.length >= 2) {
            RequestGenerator generator = new SequentialRequestGenerator(
                    Config.getInstance().getNumberOfThreads(),
                    Config.getInstance().getNumberOfRequests(),
                    args[1]
            );
            int numOfRequests = Config.getInstance().getNumberOfRequests();
            generateRequest(generator, numOfRequests);
        }
        else {
            System.out.println ("Usage: RegularClient -s <filename>");
        }
    }


    private void launchFilePropagator(String[] args) {
        if (args.length >= 3) {
            propagateFiles(args[1], args[2]);
        }
        else {
            System.out.println ("Usage: RegularClient -p <src file> <output rank file>");
        }
    }
    
    private final String CEPH_FILE_SYSTEM_PATH = "/mnt/cephfs/";

    private void writeRequest(String pathName, long fileSize) {
        //TODO - Test this new method
        try {
            long start = System.currentTimeMillis();
            File file = new File(pathName.substring(0, pathName.lastIndexOf('/')));
            file.mkdirs();
            RandomAccessFile f = new RandomAccessFile(pathName, "rw");
            f.setLength(fileSize);
            long time = System.currentTimeMillis() - start;
            System.out.printf("Took %.1f seconds to create a file of %.3f GB", time / 1e3, f.length() / 1e9);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readRequest(String pathName) {
        //TODO - Test this method
        File file = new File(pathName);

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(file));
            String st;
            while ((st = br.readLine()) != null){
                //Don't really do anything with it
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean doesFileSystemExist() {
        return new File(CEPH_FILE_SYSTEM_PATH).exists();
    }

    private void generateRequest(RequestGenerator generator, int numOfRequests) {
        int numThreads = Config.getInstance().getNumberOfThreads();

        if (!doesFileSystemExist()) {
            System.err.println("Ceph file system does not exist");
            System.exit(0);
        }

        RequestService service = new RequestService(numThreads,
                Config.getInstance().getReadWriteInterArrivalRate(),
                numOfRequests,
                generator,
                (request, threadId) -> {
                    if (generator instanceof SmartRequestGenerator) {
                        System.out.println("Request: " + request.getFilename());
                        if (request.getCommand() == Request.Command.WRITE) {
                            System.out.println("Write Request");
                            writeRequest(CEPH_FILE_SYSTEM_PATH + request.getFilename(), request.getSize());
                        }
                        else if (request.getCommand() == Request.Command.READ) {
                            System.out.println("Read Request");
                            readRequest(CEPH_FILE_SYSTEM_PATH + request.getFilename());
                        }
                    }
                });

        service.start();
        ((SmartRequestGenerator)generator).saveDynamicTreeToFile("dynamicTree.txt");
        System.exit(0);
    }

    private void generateRequestFile(String filename, String fileOut, int numOfRequests) {
        try {
            FileWriter w = new FileWriter(fileOut);
            BufferedWriter bw = new BufferedWriter(w);
            PrintWriter wr = new PrintWriter(bw, true);

            RequestGenerator generator = new ClientRequestGenerator(filename);
            int numThreads = Config.getInstance().getNumberOfThreads();
            RequestService service = new RequestService(1,
                    1,
                    numOfRequests * numThreads,
                    generator,
            this);

            service.start();
            wr.close();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.exit(0);
    }

    private void propagateFiles(String src, String rankFile) {
        FSPropagator propagator = new FSPropagator(src, rankFile, this);
        propagator.start();
        System.exit(0);
    }

    @Override
    public void onRequestGenerated(Request request, int threadId) {
        System.out.println("This is the -p callback????");
        System.out.println("thread[" + threadId + "]: " +  request);
    }
}
