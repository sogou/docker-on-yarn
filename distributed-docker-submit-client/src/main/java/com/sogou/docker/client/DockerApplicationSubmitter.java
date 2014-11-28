package com.sogou.docker.client;

import com.sogou.docker.client.model.SubmitException;
import com.sogou.docker.client.model.SubmitItem;
import com.sogou.docker.client.model.SubmitReturnItem;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

/**
 * Created by guoshiwei on 14/11/28.
 */
public class DockerApplicationSubmitter {
    private static final Log logger = LogFactory.getLog(DockerApplicationSubmitter.class);
    private final String disShellJar;

    public DockerApplicationSubmitter(String disShellJar){
        this.disShellJar = disShellJar;
    }
    public static SubmitReturnItem service(String[] args) {
        boolean result = false;
        SubmitReturnItem ret = new SubmitReturnItem();
        ApplicationId appid  = null;
        ApplicationReport report = null;
        try {
            DockerClient client = new DockerClient();
            logger.info("Initializing Client");
            try {
                boolean doRun = client.init(args);
                if (!doRun) {
                    ret.setMessage("no need to run, just help");
                    return ret;
                }
            } catch (IllegalArgumentException e) {
                System.err.println(e.getLocalizedMessage());
                client.printUsage();
                ret.setMessage(e.getLocalizedMessage());
                return ret;
            }
            appid = client.run();
            report = client.getYarnClient().getApplicationReport(appid);

        } catch (Throwable t) {
            logger.fatal("Error running CLient", t);
            ret.setMessage(t.getLocalizedMessage());
            return ret;
        }

        if (appid != null) {
            logger.info("Application complete submit successfully");
            ret.setApplicationIdObject(appid);
            ret.setAppid(appid.toString());
            ret.setApptrackingurl(report.getTrackingUrl());
            return ret;
        }
        return ret;
    }

    public SubmitReturnItem submitToYarn(SubmitItem si) {
        if (si.getCommand() == null || si.getWorkingdir() == null || si.getDocker_image() == null)
            throw new SubmitException("field command, workingdir and docker_image are needed");

        String shellArgs = "";
        shellArgs += "\" -timeout " + si.getTimeout();
        shellArgs += " -workingdir " + si.getWorkingdir();
        String[] parts = si.getCommand().split("\\s{1,}");
        for (int i = 0; i < parts.length; ++i) {
            if (parts[i] == null) continue;
            shellArgs += " -cmd_args " + parts[i] + "";
        }

        shellArgs += " -container_memory " + si.getContainer_memory();
        shellArgs += " -container_vcores " + si.getContainer_vcores();
        shellArgs += " -docker_image " + si.getDocker_image();
        if (si.isDebug()) {
            shellArgs += " -debug ";
        }
        if (si.getVirualdirs() != null) {
            for (int i = 0; i < si.getVirualdirs().length; ++i) {
                if (si.getVirualdirs()[i] == null || si.getVirualdirs()[i].trim().length() == 0) continue;
                shellArgs += " -virtualdir " + si.getVirualdirs()[i];
            }

        }
        shellArgs += "\"";
        String[] args = {"-jar", this.disShellJar
                , "-num_containers", "1", "-container_memory", String.valueOf(si.getContainer_memory()),
                "-master_memory", String.valueOf(si.getMaster_memory()), "-container_retry", String.valueOf(si.getContainer_retry())
                , "-priority", String.valueOf(si.getPriority()), "-appname", si.getAppname(), "-timeout", String.valueOf(si.getTimeout())
                , "-queue", si.getQueue()
                , "-shell_args", shellArgs};

        return service(args);
    }
}
