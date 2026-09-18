package cli;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
        name = "config",
        description = "Configure SyncFlow"
)
public class ConfigCommand implements Runnable {

    @Option(
            names = "--source",
            description = "Source PostgreSQL URL"
    )
    private String sourceUrl;

    @Option(
            names = "--target",
            description = "Target PostgreSQL URL"
    )
    private String targetUrl;

    @Override
    public void run() {

        if (sourceUrl != null) {
            System.out.println("Source: " + sourceUrl);
        }

        if (targetUrl != null) {
            System.out.println("Target: " + targetUrl);
        }

        if (sourceUrl == null && targetUrl == null) {
            System.out.println("No configuration provided");
        }
    }
}