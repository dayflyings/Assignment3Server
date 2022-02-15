package servlet;

import com.google.gson.Gson;
import model.ResortSkiers;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@WebServlet(name = "ResortSeason", urlPatterns = "/resorts/*")
public class ResortSeasonServlet extends HttpServlet {
    private Gson gson = new Gson();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        PrintWriter out = resp.getWriter();
        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");
        String pathInfo = req.getPathInfo();
        String[] pathParts = pathInfo.split("/");
        int skierLength = 7;
        int seasonLength = 3;
        if (pathParts.length != skierLength && pathParts.length != seasonLength) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            out.write("Invalid parameter.");
        } else {
            if (pathParts.length == skierLength) {
                // Handle "/resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers" pattern
                skiersNumberPath(pathParts, resp, out);
            } else {
                // Handle "/resorts/{resortID}/seasons" pattern
                seasonListPath(pathParts, resp, out);
            }
        }
        out.flush();
    }

    public boolean isNumeric(String str) {
        ParsePosition pos = new ParsePosition(0);
        NumberFormat.getInstance().parse(str, pos);
        return str.length() == pos.getIndex();
    }

    public boolean isValid(List<Integer> param) {
        Integer resortsId = param.get(0);
        Integer season = param.get(1);
        Integer day = param.get(2);
        if (resortsId != 1 && resortsId != 2) {
            return false;
        } else if (season != 2017 && season != 2018) {
            return false;
        } else return day >= 1 && day <= 365;
    }

    public void skiersNumberPath(String[] pathParts, HttpServletResponse resp, PrintWriter out) {
        Integer[] position = new Integer[]{1,3,5};
        List<Integer> parameters = new ArrayList<>();
        for (Integer p : position) {
            if (!isNumeric(pathParts[p])) {
                resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                out.write("One or more parameters are not number");
                break;
            } else {
                parameters.add(Integer.parseInt(pathParts[p]));
            }
        }
        if (!isValid(parameters)) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            out.write("One or more parameters are not valid");
        } else {
            resp.setStatus(HttpServletResponse.SC_OK);
            ResortSkiers rs = new ResortSkiers("Mission Ridge", 78999);
            out.write(gson.toJson(rs));
        }
    }

    public void seasonListPath(String[] pathParts, HttpServletResponse resp, PrintWriter out) {
        int position = 1;
        int resortID = Integer.parseInt(pathParts[position]);
        if (resortID != 1 && resortID != 2) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            out.write("resort ID is not valid");
        } else {
            String resortSeason = resortID == 1?"2017":"2018";
            Map<Integer, String> seasonMap = new HashMap<>();
            seasonMap.put(resortID, resortSeason);
            out.write(gson.toJson(seasonMap));
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        BufferedReader reader = req.getReader();
        PrintWriter out = resp.getWriter();
        String pathInfo = req.getPathInfo();
        String[] pathParts = pathInfo.split("/");
        int postLength = 3;
        if (pathParts.length != postLength || !pathParts[postLength - 1].equals("seasons")) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            out.write("Invalid inputs");
        } else {
            int resortIdPosition = 1;
            if (Integer.parseInt(pathParts[resortIdPosition]) != 1
                    && Integer.parseInt(pathParts[resortIdPosition]) != 2) {
                resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
                out.write("Resort not found");
            } else {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
                resp.setStatus(HttpServletResponse.SC_CREATED);
                out.write("New season created:" + sb);
            }
        }
    }
}
