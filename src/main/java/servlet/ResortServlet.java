package servlet;

import com.google.gson.Gson;
import model.Resort;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

@WebServlet(name = "Resort", urlPatterns = "/resorts")
public class ResortServlet extends HttpServlet {
    private Gson gson = new Gson();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        PrintWriter out = resp.getWriter();
        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");
        List<Resort> resortList = new ArrayList<>();
        resortList.add(new Resort("Whistler", 1));
        resortList.add(new Resort("Vail", 2));
        String resortListString = gson.toJson(resortList);
        out.print(resortListString);
        out.flush();
    }
}
