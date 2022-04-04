package servlet;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.lang3.concurrent.EventCountCircuitBreaker;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import util.ChannelFactory;
import util.Tools;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@WebServlet(name = "SkierServlet", urlPatterns = "/skiers/*")
public class SkierServlet extends HttpServlet {
    private Gson gson = new Gson();
    private static Connection connection = null;
    private ObjectPool<Channel> pool;
    @Override
    public void init() throws ServletException {
        super.init();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("172.31.87.118");
        factory.setUsername("test");
        factory.setPassword("test");
        try {
            connection = factory.newConnection();
            pool = new GenericObjectPool<>(new ChannelFactory(connection));
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("application/json");
        String urlPath = request.getPathInfo();
        PrintWriter out = response.getWriter();

        // check we have a URL!
        if (urlPath == null || urlPath.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            out.write("missing parameters");
        }

        String[] urlParts = urlPath.split("/");
        int seasonLength = 8;
        int verticalLength = 3;
        if (urlParts.length == seasonLength) {
            handleSeasons(urlParts, response, out);
        } else if (urlParts.length == verticalLength) {
            handleVertical(urlParts, response, out);
        } else {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            out.write("Invalid parameters");
        }
    }

    private void handleSeasons(String[] urlParts, HttpServletResponse response, PrintWriter out) {
        if (!Tools.isUrlValid(urlParts)) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            out.write("It works!");
        }
    }

    private void handleVertical(String[] urlParts, HttpServletResponse response, PrintWriter out) {
        if (!Tools.isNumeric(urlParts[1])) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            out.write("Data not found");
        } else {
            int skierId = Integer.parseInt(urlParts[1]);
            if (skierId < 0) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                out.write("Data not found");
            } else {
                response.setStatus(HttpServletResponse.SC_OK);
                Map<String, Object> result = new HashMap<>();
                result.put("seasonID", "string");
                result.put("totalVert", 0);
                out.write(gson.toJson(result));
            }
        }
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        BufferedReader reader = request.getReader();
        PrintWriter out = response.getWriter();
        String urlPath = request.getPathInfo();
        EventCountCircuitBreaker breaker =
                new EventCountCircuitBreaker(500, 1, TimeUnit.SECONDS, 300);
        if (urlPath == null || urlPath.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().write("missing paramterers");
        }
        String[] urlParts = urlPath.split("/");
        if (!Tools.isUrlValid(urlParts)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            out.write("Invalid inputs");
        } else {
            int resortIdPosition = 1;
            int seasonIdPosition = 3;
            int dayIdPosition = 5;
            int skierIdPosition = 7;
            int resortId = Integer.parseInt(urlParts[resortIdPosition]);
            int seasonId = Integer.parseInt(urlParts[seasonIdPosition]);
            int dayId = Integer.parseInt(urlParts[dayIdPosition]);
            int skierId = Integer.parseInt(urlParts[skierIdPosition]);
            if ((resortId != 1 && resortId != 2) ||
                    (seasonId != 2017 && seasonId != 2018) ||
                    (dayId < 1 || dayId > 365)) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                out.write("Data not found");
            } else {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
                reader.close();
                String[] resList = sb.toString().split(",");
                if (resList.length == 3) {
                    int time = Integer.parseInt(resList[0]);
                    int liftId = Integer.parseInt(resList[1]);
                    int waitTime = Integer.parseInt(resList[2]);
                    if (breaker.incrementAndCheckState()) {
                        response.setStatus(HttpServletResponse.SC_CREATED);
                        sendMessageToQueue(resortId, seasonId, dayId, skierId, time, liftId, waitTime);
                        out.write("Write successful: \n" + sb);
                    } else {
                        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                        out.write("Sever busy.");
                    }
                } else {
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    out.write("Missing body");
                }
            }
        }
    }

    private void sendMessageToQueue(int resortId, int seasonId, int dayId, int skierId, int time, int liftId, int waitTime) {
        Channel channel = null;
        try {
            if (connection == null) {
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("172.31.87.118");
                factory.setUsername("test");
                factory.setPassword("test");
                connection = factory.newConnection();
                pool = new GenericObjectPool<>(new ChannelFactory(connection));
            }
            channel = pool.borrowObject();
            channel.queueDeclare("Hello", false, false, false, null);
            channel.queueDeclare("Resort", false, false, false, null);
            String message = resortId + "\n" + seasonId + "\n" + dayId + "\n" + skierId + "\n" + time + "\n" + liftId + "\n" + waitTime;
            channel.basicPublish("", "Hello", null, message.getBytes());
            channel.basicPublish("", "Resort", null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (channel != null) {
                    pool.returnObject(channel);
                }
            } catch (Exception ignored) {
            }
        }
    }
}
