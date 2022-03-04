package servlet;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
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
import java.util.concurrent.TimeoutException;

@WebServlet(name = "SkierServlet", urlPatterns = "/skiers/*")
public class SkierServlet extends HttpServlet {
    private Gson gson = new Gson();
    private static Connection connection = null;
    private final ThreadLocal<Channel> channels = new ThreadLocal<>();
    @Override
    public void init() throws ServletException {
        super.init();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("172.31.87.118");
        factory.setUsername("test");
        factory.setPassword("test");
        try {
            connection = factory.newConnection();
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
                    sb.append(line + "\n");
                }
                response.setStatus(HttpServletResponse.SC_CREATED);
                sendMessageToQueue(resortId, seasonId, dayId, skierId);
                out.write("Write successful:" + sb);
            }
        }
    }

    private void sendMessageToQueue(int resortId, int seasonId, int dayId, int skierId) {
        try  {
            Channel channel = channels.get();
            if (channel == null) {
                channel = connection.createChannel();
                channels.set(channel);
            }
            channel.queueDeclare("Hello", false, false, false, null);
            String message = resortId + "\n" + seasonId + "\n" + dayId + "\n" + skierId;
            channel.basicPublish("", "Hello", null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
