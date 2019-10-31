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

    String runCommand(String command) {
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("bash", "-c", command);
        try {
            Process process = processBuilder.start();
            StringBuilder output = new StringBuilder();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line + "\n");
            }

            int exitVal = process.waitFor();
            if (exitVal == 0) {
                return output.toString();
            } else {
                //Error
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return "";
    }

    final String CEPH_FILE_SYSTEM_PATH = "/mnt/cephfs/";

    void writeRequest(String folderPath, String fileName, long fileSize) {
        String createDirectory = "mkdir -p " + CEPH_FILE_SYSTEM_PATH + folderPath;
        System.out.println("Create directory command: "+ createDirectory);
        runCommand(createDirectory);

        String createFile = "truncate -s " + fileSize + " " + CEPH_FILE_SYSTEM_PATH + folderPath + "/" + fileName;
        System.out.println("Create file command: " + createFile);
        runCommand(createFile);
    }

    private void generateRequest(RequestGenerator generator, int numOfRequests) {
        int numThreads = Config.getInstance().getNumberOfThreads();

        RequestService service = new RequestService(numThreads,
                Config.getInstance().getReadWriteInterArrivalRate(),
                numOfRequests,
                generator,
                new RequestThread.RequestGenerateThreadCallBack() {
                    @Override
                    public void onRequestGenerated(Request request, int threadId) {
                        if (generator instanceof SmartRequestGenerator) {

                            System.out.println("Request: " + request.getFilename());

                            String filename = request.getFilename();
                            filename = filename.replaceAll("\\\\", "/");
                            String[] splitArr = filename.split("/");
                            StringBuilder folderPath =new StringBuilder();
                            int count=0;
                            while(count<splitArr.length - 1) {
                                folderPath.append(splitArr[count]+"/");
                                count++;
                            }
                            folderPath.deleteCharAt(folderPath.length()-1);

                            String fileName = splitArr[splitArr.length-1];
                            System.out.println("Folder path: " + folderPath);
                            System.out.println("File name: " +  fileName);
                            System.out.println("");

                            if (request.getCommand() == Request.Command.WRITE) {
                                System.out.println("Write Request");
                                writeRequest(folderPath.toString(), fileName, request.getSize());
                            }
                            else if (request.getCommand() == Request.Command.READ) {

                            }
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
