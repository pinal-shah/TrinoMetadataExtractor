/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.trino.cli;

import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import static org.apache.atlas.trino.cli.ExtractorContext.*;

@DisallowConcurrentExecution // Prevents overlapping execution
public class TrinoExtractorMain {
    private static final Logger LOG = LoggerFactory.getLogger(TrinoExtractorMain.class);

    private static final int EXIT_CODE_SUCCESS = 0;
    private static final int EXIT_CODE_FAILED = 1;
    private static final int EXIT_CODE_HELP = 2;
    private static final Pattern CRON_PATTERN = Pattern.compile("^([0-5]?\\d|\\*) ([0-5]?\\d|\\*) ([01]?\\d|2[0-3]|\\*) ([1-9]|[12]\\d|3[01]|\\*) ([1-9]|1[0-2]|\\*) ([0-7]|\\*)$");
    private static ExtractorContext extractorContext;
    static int exitCode = EXIT_CODE_FAILED;

    public static void main(String[] args) {
        try {
            extractorContext = createExtractorContext(args);

            if (extractorContext != null) {
                String cronExpression = extractorContext.getCronExpression();
                if (StringUtils.isNotEmpty(cronExpression)) {

                    if (!isValidCronExpression(cronExpression)) {
                        exitCode = EXIT_CODE_FAILED;
                        LOG.error("Invalid cron expression provided: {}", cronExpression);

                    } else {
                        JobDetail job = JobBuilder.newJob(MetadataJob.class).build();
                        Trigger trigger = TriggerBuilder.newTrigger()
                                .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
                                .build();

                        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
                        scheduler.start();
                        scheduler.scheduleJob(job, trigger);
                    }

                } else {
                    ExtractorService extractorService = new ExtractorService();
                    if (extractorService.execute(extractorContext)) {
                        exitCode = EXIT_CODE_SUCCESS;
                        LOG.info("Successfully completed execution of extraction task.");
                    }
                }
            } else {
                exitCode = EXIT_CODE_HELP;
            }
        } catch (Exception e) {
            LOG.error("Error encountered.", e);
            System.out.println(e.getMessage());

        } finally {
            if (extractorContext != null && extractorContext.getAtlasConnector() != null) {
                extractorContext.getAtlasConnector().close();
            }
        }
        System.exit(exitCode);
    }

    static ExtractorContext createExtractorContext(String[] args) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Initializing the extractor context.");
        }

        Options acceptedCliOptions = prepareCommandLineOptions();

        try {
            CommandLine cmd = new BasicParser().parse(acceptedCliOptions, args, true);
            List<String> argsNotProcessed = cmd.getArgList();

            if (argsNotProcessed != null && argsNotProcessed.size() > 0) {
                throw new AtlasBaseException("Unrecognized arguments.");
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Successfully initialized the extractor context.");
            }

            ExtractorContext ret = null;
            if (cmd.hasOption(ExtractorContext.OPTION_HELP_SHORT)) {
                printUsage(acceptedCliOptions);
            } else {
                ret = new ExtractorContext(cmd);
            }

            return ret;
        } catch (ParseException | AtlasBaseException e) {
            printUsage(acceptedCliOptions);

            throw new AtlasBaseException("Invalid arguments. Reason: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }


    private static Options prepareCommandLineOptions() {
        Options acceptedCliOptions = new Options();

        return acceptedCliOptions.addOption(OPTION_CATALOG_SHORT, OPTION_CATALOG_LONG, true, "Catalog name")
                .addOption(OPTION_SCHEMA_SHORT, OPTION_SCHEMA_LONG, true, "Schema name")
                .addOption(OPTION_TABLE_SHORT, OPTION_TABLE_LONG, true, "Table name")
                .addOption(OPTION_CRON_EXPRESSION_SHORT, OPTION_CRON_EXPRESSION_LONG, true, "Cron expression to run extraction")
                .addOption(OPTION_FAIL_ON_ERROR, false, "failOnError")
                .addOption(OPTION_DELETE_NON_EXISTING, false, "Delete database and table entities in Atlas if not present in Hive")
                .addOption(OPTION_HELP_SHORT, OPTION_HELP_LONG, false, "Print this help message");
    }

    //TO-DO
    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        System.out.println();
    }

    private static boolean isValidCronExpression(String cronExpression) {
        return CRON_PATTERN.matcher(cronExpression).matches();
    }

    public class MetadataJob implements Job {
        private final Logger LOG = LoggerFactory.getLogger(MetadataJob.class);

        public void execute(JobExecutionContext context) {
            LOG.info("Executing metadata extraction at: {}", java.time.LocalTime.now());

            if (extractorContext != null) {
                try {
                    ExtractorService extractorService = new ExtractorService();
                    extractorService.execute(extractorContext);
                    exitCode = EXIT_CODE_SUCCESS;
                    LOG.info("Successfully completed execution of extraction task.");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                exitCode = EXIT_CODE_FAILED;
                LOG.error("Extractor Context not found, Skipping execution");
            }
        }
    }
}
