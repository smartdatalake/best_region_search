package SDL.main;

import com.google.gson.Gson;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.*;
import java.sql.*;
import java.util.LinkedList;

import static spark.Spark.get;
import static spark.Spark.port;

public class main {
    static LinkedList<Inputs> buffer = new LinkedList<>();
    static Conf conf = new Conf();


    public static void getCSVfromProteus(String tableName, String column, String f, char delimiter, String pathToDirectory, String serverURL, String username, String password) throws Exception {
        System.out.println("fetching table from proteus " + tableName + column);
        Class.forName("org.apache.calcite.avatica.remote.Driver");
        Connection connection = DriverManager.getConnection(serverURL, username, password);
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select lat,lon" + ((column.equals("null")) ? "" : ("," + column)) + ((f.equals("null")) ? ("") : "," + f) + " from " + tableName + " where lat is not null and lon is not null");
        ResultSetMetaData rsmd = rs.getMetaData();
        String header = "";
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            header += rsmd.getColumnName(i) + delimiter;
        header = header.substring(0, header.length() - 1);
        int numberOfColumns = rsmd.getColumnCount();
        BufferedWriter writer = new BufferedWriter(new FileWriter(pathToDirectory + tableName + column + f + ".csv"));
        writer.write(header + "\n");
        while (rs.next()) {
            String row = "";
            for (int i = 1; i <= numberOfColumns; i++) {
                String value = rs.getString(i);
                if (rs.getObject(i) != null)
                    row += value.replace(delimiter, ' ');
                if (i < numberOfColumns)
                    row += delimiter;
            }
            writer.write(row + "\n");
        }
        writer.close();
        System.out.println("fetched table from proteus " + tableName + column);
    }

    public static void main(String[] args) throws Exception {
        Gson gson = new Gson();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(
                    "buffer.tmp"));
            String line = reader.readLine();
            while (line != null) {
                buffer.add(gson.fromJson(line, Inputs.class));
                line = reader.readLine();
            }
            reader.close();

            reader = new BufferedReader(new FileReader(
                    "conf.txt"));
            conf = gson.fromJson(reader, Conf.class);
            if (conf.port < 0 || conf.dataPath == "" || conf.algo < 0 || conf.partitionCNT < 1 || conf.partitionCNT > 100000 || conf.SparkMaster == "" || conf.sparkSubmit == "")
                throw new Exception("Invalid configuration");

        } catch (IOException e) {
            e.printStackTrace();
        }
        port(conf.port);

        get("/alive", (req, res) -> {
            try {
                if (isSparkFree("http://" + conf.getSparkMaster() + ":8080"))
                    return "I am alive, and Spark cluster is free.";
                else
                    return "I am alive, but Spark cluster is not free.";
            } catch (Exception e) {
                return "I am alive, but there is error in Spark Cluster:\n" + e.toString();
            }
        });

        get("/changeProteus", (req, res) -> {
            String address, username, pass;
            try {
                address = req.queryParamOrDefault("url", "");
                username = req.queryParamOrDefault("username", "");
                pass = req.queryParamOrDefault("pass", "");
            } catch (Exception e) {
                return e;
            }
            if (address.equals("") || username.equals("") || pass.equals(""))
                return "blank url, username, or password.";
            conf.ProteusURL = address;
            conf.ProteusUsername = username;
            conf.ProteusPassword = pass;
            return "Proteus URL and credentials are updated.";
        });

        get("/flushBuffer", (req, res) -> {
            buffer.clear();
            PrintWriter writer = new PrintWriter("buffer.tmp");
            writer.print("");
            writer.close();
            return "buffer.tmp is flushed.";
        });

        get("/BRS", (request, response) -> {
            response.type("application/json");
            int topK;
            double eps;
            String f, table, keywordsColumn, keywords, keywordsColumn2, keywords2;
            Boolean dist;
            try {
                dist = Boolean.parseBoolean(request.queryParamOrDefault("dist", "true"));
                topK = Integer.parseInt(request.queryParamOrDefault("topk", "0"));
                eps = Double.parseDouble(request.queryParamOrDefault("eps", "0"));
                f = request.queryParamOrDefault("f", "null");
                table = request.queryParamOrDefault("input", "null");
                keywordsColumn = request.queryParamOrDefault("keywordsColumn", "null");
                keywords = request.queryParamOrDefault("keywords", "null");
                keywordsColumn2 = request.queryParamOrDefault("keywordsColumn2", "null");
                keywords2 = request.queryParamOrDefault("keywords2", "null");
            } catch (Exception e) {
                return e;
            }
            Inputs ins = new Inputs(topK, eps, f, keywordsColumn, keywords, keywordsColumn2, keywords2, table, dist);
            if (table == null)
                return "Define an input table.";
            if (topK <= 0)
                return "Invalid topK value.";
            if (eps <= 0)
                return "Invalid eps value.";
            if (keywords == "null" ^ keywordsColumn == "null")
                return "Define both keywordColumn and keywords; otherwise let both null.";
            if (keywords2 == "null" ^ keywordsColumn2 == "null")
                return "Define both keywordColumn and keywords; otherwise let both null.";

            for (Inputs input : buffer)
                if (ins.equals(input)) {
                    if (input.error == "" && input.out == "")
                        return "Your query is under process.";
                    if (!input.error.equals(""))
                        return input.error;
                    if (!input.out.equals(""))
                        return input.out;
                    else
                        return "Unable to answer the query. Please, stop BRS, check your query, and empty buffer.tmp " +
                                "!!! Caution, you will lost the cached results)";
                }
            buffer.add(ins);
            try {
                if (isSparkFree("http://" + conf.getSparkMaster() + ":8080")) {
                    // cal();
                    return "Your query is submitted on the server. Ping me again for results.";
                }
            } catch (IOException e) {
                response.status(404);
                if (e.getMessage().contains("Connection refused (Connection refused)"))
                    return "Spark Server is down";
                if (e.getMessage().contains("Server is busy")) {
                    return "Server is busy. Your request is under process.";
                }
                return "Unknown error, Please call the admin.";
            } catch (Exception e) {
                return e.getMessage();
            }
            return "Fatal error. Please call the admin.";
        });
        while (1 > -1) {
            Thread.sleep(10000);
            cal();
        }
    }

    public static void cal() throws Exception {
        Process p;
        Gson gson = new Gson();
        for (int i = 0; i < buffer.size(); i++) {
            try {
                Inputs ins = buffer.get(i);
                if (ins.error == "" && ins.out == "") {
                    File[] listOfFiles = new File(conf.dataPath).listFiles();
                    boolean getTable = true;
                    for (int j = 0; j < listOfFiles.length; j++)
                        if (listOfFiles[j].isFile() && listOfFiles[j].getName().equals(ins.input + ins.keywordsColumn + ins.f + ".csv"))
                            getTable = false;
                    if (getTable) {
                        getCSVfromProteus(ins.input, ins.keywordsColumn, ins.f, conf.delimiter, conf.dataPath, conf.ProteusURL, conf.ProteusUsername, conf.ProteusPassword);
                    }
                    if (isSparkFree("http://" + conf.getSparkMaster() + ":8080")) {
                        Long time = System.nanoTime();
                        System.out.println((conf.getSparkSubmit() + " --executor-memory 50g --driver-memory 50g --conf spark.buffer.pageSize=2m --class SDL.main.Run --packages org.locationtech.jts:jts-core:1.16.0 --master " +
                                "spark://" + conf.SparkMaster + ":7077 " + conf.jarPath + " " + conf.SparkMaster + " " + ins.topk + " " + ins.eps
                                + " " + ins.partitionsCNT + " " + conf.algo + " " + ins.base + " " + ins.Kprime + " " + ins.f
                                + " " + ins.keywordsColumn + " " + ins.keywords + " " + conf.dataPath + ins.input + ins.keywordsColumn + ins.f + ins.keywordsColumn2 + ".csv"
                                + " " + ins.dist + " " + ins.keywordsColumn2 + " " + ins.keywords2));
                        if (ins.dist == false && (conf.algo == 2 || conf.algo == 1))
                            p = Runtime.getRuntime().exec((conf.getSparkSubmit() + " --executor-memory 50g --driver-memory 50g --conf spark.buffer.pageSize=2m --class SDL.main.Run --packages org.locationtech.jts:jts-core:1.16.0 --master " +
                                    "spark://" + conf.SparkMaster + ":7077 " + conf.jarPath + " local[*]  " + ins.topk + " " + ins.eps
                                    + " " + ins.partitionsCNT + " " + 0 + " " + ins.base + " " + ins.Kprime + " " + ins.f
                                    + " " + ins.keywordsColumn + " " + ins.keywords + " " + conf.dataPath + ins.input + ins.keywordsColumn + ins.f + ins.keywordsColumn2 + ".csv"
                                    + " " + ins.dist + " " + ins.keywordsColumn2 + " " + ins.keywords2));
                        else
                            p = Runtime.getRuntime().exec((conf.getSparkSubmit() + " --executor-memory 50g --driver-memory 50g --conf spark.buffer.pageSize=2m --class SDL.main.Run --packages org.locationtech.jts:jts-core:1.16.0 --master " +
                                    "spark://" + conf.SparkMaster + ":7077 " + conf.jarPath + " local[*]  " + ins.topk + " " + ins.eps
                                    + " " + ins.partitionsCNT + " " + conf.algo + " " + ins.base + " " + ins.Kprime + " " + ins.f
                                    + " " + ins.keywordsColumn + " " + ins.keywords + " " + conf.dataPath + ins.input + ins.keywordsColumn + ins.f + ins.keywordsColumn2 + ".csv"
                                    + " " + ins.dist + " " + ins.keywordsColumn2 + " " + ins.keywords2));
                        BufferedReader br = new BufferedReader(
                                new InputStreamReader(p.getInputStream()));
                        BufferedReader brErr = new BufferedReader(
                                new InputStreamReader(p.getErrorStream()));
                        String s = "";
                        String err = "";
                        String out = "";
                        while ((s = br.readLine()) != null)
                            out += s + "\n";
                        while ((s = brErr.readLine()) != null)
                            err += s + "\n";

                        p.waitFor();
                        p.destroy();
                        int index = out.indexOf("[\n{");
                        if (index > 0)
                            ins.out = out.substring(out.indexOf("[\n{"));
                        if (err.contains("Exception in thread \"main\""))
                            ins.error = err;
                        if (ins.error.contains("Path does not exist")) {
                            ins.error = "Table not found";
                        }
                        if (ins.error.contains("given input columns: [")) {
                            ins.error = "Column not found";
                        }
                        System.out.println("OUT:\n" + ins.out);
                        System.out.println("ERROR:\n" + ins.error);
                        System.out.println("Time:\n" + (System.nanoTime() - time) / 1000000000);
                        buffer.set(i, ins);
                    }
                }
            } catch (Exception e) {
                System.out.println(buffer.get(i).toString() + "!!!!!!!!!!" + e);
            }
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter("buffer.tmp"));
        for (Inputs b : buffer)
            writer.write(gson.toJson(b) + "\n");
        writer.close();
    }

    public static class Inputs implements Serializable {
        int topk = 0;
        double eps = 0.0;
        int partitionsCNT = 100;
        int algo = 2;
        int base = 16;
        int Kprime = 5;
        String f = "";
        String keywordsColumn = "";
        String keywords = "";
        String keywordsColumn2 = "";
        String keywords2 = "";
        String input = "";
        String error = "";
        String out = "";
        Boolean dist = true;

        public Inputs(int topk, double eps, String f, String keywordsColumn, String keywords, String keywordsColumn2, String keywords2, String input, Boolean dist) {
            this.topk = topk;
            this.eps = eps;
            this.f = f;
            this.keywordsColumn = keywordsColumn;
            this.keywords = keywords;
            this.keywordsColumn2 = keywordsColumn2;
            this.keywords2 = keywords2;
            this.input = input;
            this.dist = dist;
        }

        public Inputs(int topk, double eps, String f, String keywordsColumn, String keywords, String input) {
            this.topk = topk;
            this.eps = eps;
            this.f = f;
            this.keywordsColumn = keywordsColumn;
            this.keywords = keywords;
            this.input = input;
        }

        public Inputs() {
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || !Inputs.class.isAssignableFrom(o.getClass()))
                return false;
            final Inputs other = (Inputs) o;
            if (this.eps == other.eps && this.f.equals(other.f) && this.input.equals(other.input) && this.keywords.equals(other.keywords)
                    && this.keywordsColumn.equals(other.keywordsColumn) && this.keywords2.equals(other.keywords2)
                    && this.keywordsColumn2.equals(other.keywordsColumn2) && this.topk == other.topk)
                return true;
            return false;
        }
    }

    public static class BufferTemplate implements Serializable {
        Inputs ins;
        String error = "";
        Region[] res;
    }

    public class Region {
        int rank = 0;
        Double[] coord = new Double[2];
        int score = -1;
    }

    public static class Conf {
        String sparkSubmit = "";
        String jarPath = "";
        String SparkMaster = "";
        int partitionCNT = 1600;
        int algo = 2;
        String dataPath = "";
        int port = 4646;
        char delimiter = ',';
        String ProteusUsername = "";
        String ProteusPassword = "";
        String ProteusURL = "";

        Conf() {
            this.sparkSubmit = "";
            this.SparkMaster = "";
            int partitionCNT = 1600;
            this.algo = 2;
            this.delimiter = ',';
            this.ProteusPassword = "";
            this.ProteusURL = "";
            this.ProteusUsername = "";
        }

        public Conf(String sparkSubmit, String BRSJarPath, String sparkMaster, int partitionCNT) {
            this.sparkSubmit = sparkSubmit;
            this.SparkMaster = sparkMaster;
            this.partitionCNT = partitionCNT;
        }

        public Conf(String sparkSubmit, String sparkMaster, int partitionCNT, int algo) {
            this.sparkSubmit = sparkSubmit;
            SparkMaster = sparkMaster;
            this.partitionCNT = partitionCNT;
            this.algo = algo;
        }

        public Conf(String sparkSubmit, String sparkMaster, int partitionCNT, int algo, String dataPath, int port
                , char delimiter, String proteusPassword, String proteusURL, String proteusUsername) {
            this.sparkSubmit = sparkSubmit;
            SparkMaster = sparkMaster;
            this.partitionCNT = partitionCNT;
            this.algo = algo;
            this.dataPath = dataPath;
            this.port = port;
            this.delimiter = delimiter;
            this.ProteusPassword = proteusPassword;
            this.ProteusURL = proteusURL;
            this.ProteusUsername = proteusUsername;
        }

        public String getSparkSubmit() {
            return sparkSubmit;
        }


        public String getSparkMaster() {
            return SparkMaster;
        }

        public int getPartitionCNT() {
            return partitionCNT;
        }

        public int getAlgo() {
            return algo;
        }
    }

    private static boolean isSparkFree(String sparkURL) throws IOException {
        Document doc = Jsoup.connect(sparkURL).get();
        if (Integer.parseInt(doc.childNode(1).childNode(2).childNode(1).childNode(3).childNode(1).childNode(1)
                .childNode(9).childNode(1).toString().replace(' ', '0')) == 0)
            return true;
        throw new IOException("Server is busy");
    }
}
