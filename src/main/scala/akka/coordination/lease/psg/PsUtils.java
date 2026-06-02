package akka.coordination.lease.psg;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class PsUtils {

    public static String killStop() {
        StringBuilder sb = new StringBuilder();
        try {
            long pid = ProcessHandle.current().pid();
            String[] cmd = new String[]{"kill", "-stop", String.valueOf(pid)};

            sb.append("\n");
            sb.append("===================");
            sb.append(cmd[0] + " " + cmd[1] + " " + cmd[2]);
            sb.append("===================");
            sb.append("\n");
            Process ps = Runtime.getRuntime().exec(cmd);
            try (BufferedReader input = new BufferedReader(new InputStreamReader(ps.getInputStream()))) {
                String line;
                while ((line = input.readLine()) != null) {
                    sb.append(line).append('\n');
                }
            }
            return sb.toString();
        } catch (Throwable e) {
            return sb.toString();
        }
    }
}
