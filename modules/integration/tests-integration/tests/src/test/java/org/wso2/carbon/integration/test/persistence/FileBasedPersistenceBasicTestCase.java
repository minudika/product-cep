/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.carbon.integration.test.persistence;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.test.utils.common.TestConfigurationProvider;
import org.wso2.carbon.integration.common.admin.client.LogViewerClient;
import org.wso2.carbon.integration.common.utils.mgt.ServerConfigurationManager;
import org.wso2.carbon.logging.view.stub.types.carbon.LogEvent;
import org.wso2.cep.integration.common.utils.CEPIntegrationTest;

import java.io.File;

/**
 * Deploying artifacts using given config files (of type XML/JSON), in typical deployment order.
 */
public class FileBasedPersistenceBasicTestCase extends CEPIntegrationTest {

    private static final Log log = LogFactory.getLog(FileBasedPersistenceBasicTestCase.class);
    private static int eventStreamCount;
    private static int eventReceiverCount;
    private static int eventPublisherCount;
    private static int executionPlanCount;
    private static final String EVENT_PROCESSING_FILE = "event-processor.xml";
    private static final String RESOURCE_LOCATION = TestConfigurationProvider.getResourceLocation()  + "artifacts" + File.separator + "CEP"
            + File.separator + "persistence";
    private static LogViewerClient logViewerClient;

    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_ADMIN);

        ServerConfigurationManager serverManager = new ServerConfigurationManager(cepServer);
        String CARBON_HOME = serverManager.getCarbonHome();
        String eventProcessingFileLocation = RESOURCE_LOCATION + File.separator + EVENT_PROCESSING_FILE;
        String cepEventProcessorFileLocation = CARBON_HOME + File.separator + "repository" + File.separator
                + "conf" + File.separator + EVENT_PROCESSING_FILE;
        serverManager.applyConfiguration(new File(eventProcessingFileLocation), new File(cepEventProcessorFileLocation), true, true);

        String loggedInSessionCookie = getSessionCookie();

        eventReceiverAdminServiceClient = configurationUtil.getEventReceiverAdminServiceClient(backendURL, loggedInSessionCookie);
        eventProcessorAdminServiceClient = configurationUtil.getEventProcessorAdminServiceClient(backendURL, loggedInSessionCookie);
        eventStreamManagerAdminServiceClient = configurationUtil.getEventStreamManagerAdminServiceClient(backendURL, loggedInSessionCookie);
        eventPublisherAdminServiceClient = configurationUtil.getEventPublisherAdminServiceClient(backendURL, loggedInSessionCookie);
        logViewerClient = new LogViewerClient(backendURL, loggedInSessionCookie);
    }

    @Test(groups = {"wso2.cep"}, description = "Testing whether artifacts get activated properly upon deployment.")
    public void addArtifactsTestScenario() throws Exception {
        eventStreamCount = eventStreamManagerAdminServiceClient.getEventStreamCount();
        eventReceiverCount = eventReceiverAdminServiceClient.getActiveEventReceiverCount();
        eventPublisherCount = eventPublisherAdminServiceClient.getActiveEventPublisherCount();
        executionPlanCount = eventProcessorAdminServiceClient.getExecutionPlanConfigurationCount();

        //Add StreamDefinition
        String pizzaStreamDefinition = getJSONArtifactConfiguration("DeployArtifactsBasicTestCase", "org.wso2.sample.pizza.order_1.0.0.json");
        eventStreamManagerAdminServiceClient.addEventStreamAsString(pizzaStreamDefinition);
        Assert.assertEquals(eventStreamManagerAdminServiceClient.getEventStreamCount(), ++eventStreamCount);

        //Add another StreamDefinition
        String outStreamDefinition = getJSONArtifactConfiguration("DeployArtifactsBasicTestCase", "outStream_1.0.0.json");
        eventStreamManagerAdminServiceClient.addEventStreamAsString(outStreamDefinition);
        Assert.assertEquals(eventStreamManagerAdminServiceClient.getEventStreamCount(), ++eventStreamCount);

        //Add HTTP EventReceiver
        String eventReceiverConfig = getXMLArtifactConfiguration("DeployArtifactsBasicTestCase", "PizzaOrder.xml");
        eventReceiverAdminServiceClient.addEventReceiverConfiguration(eventReceiverConfig);
        Assert.assertEquals(eventReceiverAdminServiceClient.getActiveEventReceiverCount(), ++eventReceiverCount);

        //Add HTTP Publisher
        String eventPublisherConfig = getXMLArtifactConfiguration("DeployArtifactsBasicTestCase", "PizzaDeliveryNotification.xml");
        eventPublisherAdminServiceClient.addEventPublisherConfiguration(eventPublisherConfig);
        Assert.assertEquals(eventPublisherAdminServiceClient.getActiveEventPublisherCount(), ++eventPublisherCount);

        //Add execution plan
        String executionPlan = getExecutionPlanFromFile("DeployArtifactsBasicTestCase", "testPlanwithWindow.siddhiql");
        eventProcessorAdminServiceClient.addExecutionPlan(executionPlan);
        Assert.assertEquals(eventProcessorAdminServiceClient.getExecutionPlanConfigurationCount(), ++executionPlanCount);

        int beforeCount = logViewerClient.getAllRemoteSystemLogs().length;
        try {
            boolean mappingPortionFound = false;
            Thread.sleep(70000);
            LogEvent[] logs = logViewerClient.getAllRemoteSystemLogs();
            for (int i = 0; i < (logs.length - beforeCount); i++) {
                if (logs[i].getMessage().contains("Snapshot taken of Execution Plan 'testPlanwithWindow'")) {
                    mappingPortionFound = true;
                    break;
                }
            }
            Assert.assertTrue(mappingPortionFound, "Siddhi state persistence does not work as expected! ");
            Thread.sleep(2000);
        } catch (Throwable e) {
            log.error("Exception thrown: " + e.getMessage(), e);
            Assert.fail("Exception: " + e.getMessage());
        }
    }

    @Test(groups = {"wso2.cep"}, description = "Removing artifacts." ,dependsOnMethods = {"addArtifactsTestScenario"} )
    public void removeArtifactsTestScenario() throws Exception {
        eventStreamManagerAdminServiceClient.removeEventStream("org.wso2.sample.pizza.order","1.0.0");
        eventStreamManagerAdminServiceClient.removeEventStream("outStream","1.0.0");
        Assert.assertEquals(eventStreamManagerAdminServiceClient.getEventStreamCount(), eventStreamCount - 2);

        eventReceiverAdminServiceClient.removeInactiveEventReceiverConfiguration("PizzaOrder.xml");
        Assert.assertEquals(eventReceiverAdminServiceClient.getActiveEventReceiverCount(), eventReceiverCount - 1);

        eventPublisherAdminServiceClient.removeInactiveEventPublisherConfiguration("PizzaDeliveryNotification.xml");
        Assert.assertEquals(eventPublisherAdminServiceClient.getActiveEventPublisherCount(), eventPublisherCount - 1);

        eventProcessorAdminServiceClient.removeInactiveExecutionPlan("testPlanwithWindow.siddhiql");
        Assert.assertEquals(eventProcessorAdminServiceClient.getExecutionPlanConfigurationCount(), executionPlanCount - 1);
    }

    @AfterClass(alwaysRun = true)
    public void destroy() throws Exception {
        super.cleanup();
    }
}
