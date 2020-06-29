package SDL.main;

import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

public class BRSInvoker {
    public static void CalBRS(String master,String jarFilePath,String[] listOfParam) throws IOException, InterruptedException {
        SparkLauncher launcher = new SparkLauncher();
        launcher.setMaster(master)
                .setAppResource(jarFilePath) // Specify user app jar path
                .setMainClass("SDL.main.Run");

            // Set app args
            launcher.addAppArgs(listOfParam);


        // Launch the app
        Process process = launcher.launch();
        process.getErrorStream().read();
        process.getInputStream().read();
        // Get Spark driver log
        //  new Thread(new ISRRunnable(process.getErrorStream())).start();
        int exitCode = process.waitFor();
        System.out.println("Finished! Exit code is "  + exitCode);
    }
}
