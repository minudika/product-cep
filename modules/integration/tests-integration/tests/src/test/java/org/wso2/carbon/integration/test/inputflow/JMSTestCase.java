/*
*  Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.carbon.integration.test.inputflow;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.extensions.servers.jmsserver.controller.JMSBrokerController;
import org.wso2.carbon.automation.extensions.servers.jmsserver.controller.config.JMSBrokerConfiguration;
import org.wso2.carbon.automation.extensions.servers.jmsserver.controller.config.JMSBrokerConfigurationProvider;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.carbon.integration.common.utils.mgt.ServerConfigurationManager;
import org.wso2.carbon.integration.test.client.JMSPublisherClient;
import org.wso2.carbon.integration.test.client.Wso2EventServer;
import org.wso2.cep.integration.common.utils.CEPIntegrationTest;
import org.wso2.cep.integration.common.utils.CEPIntegrationTestConstants;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

/**
 * Sending different formatted events to the JMS Receiver according to the receivers mapping type.
 */
public class JMSTestCase extends CEPIntegrationTest {
    private static final Log log = LogFactory.getLog(JMSTestCase.class);
    private final String GERONIMO_J2EE_MANAGEMENT = "geronimo-j2ee-management_1.1_spec-1.0.1.jar";
    private final String SENSOR_STREAM_JSON = "org.wso2.event.sensor.stream_1.0.0.json";
    private final String SENSOR_STREAM = "org.wso2.event.sensor.stream:1.0.0";
    private final String JMS_RECEIVER_MAP = "jmsReceiverMap.xml";
    private final String WSO2_EVENT_PUBLISHER = "wso2EventPublisher.xml";
    private final String ACTIVEMQ_CORE = "activemq-core-5.7.0.jar";
    private JMSBrokerController activeMqBroker = null;
    private ServerConfigurationManager serverManager = null;

    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.init(TestUserMode.SUPER_TENANT_ADMIN);
        try {
            serverManager = new ServerConfigurationManager(cepServer);
        } catch (MalformedURLException e) {
            throw new RemoteException("Malformed URL exception thrown when initializing ActiveMQ broker", e);
        }

        setupActiveMQBroker();
        try {
            // Copy dependency activemq jar files to component/lib.
            String JAR_LOCATION = File.separator + "artifacts" + File.separator + "CEP" + File.separator + "jar";
            serverManager.copyToComponentLib(new File(getClass().getResource(JAR_LOCATION + File.separator +
                    ACTIVEMQ_CORE).toURI()));
            serverManager.copyToComponentLib(new File(getClass().getResource(JAR_LOCATION + File.separator +
                    GERONIMO_J2EE_MANAGEMENT).toURI()));
            serverManager.restartGracefully();
        } catch (IOException e) {
            throw new RemoteException("IOException when initializing ActiveMQ broker", e);
        } catch (URISyntaxException e) {
            throw new RemoteException("URISyntaxException when initializing ActiveMQ broker", e);
        } catch (Exception e) {
            throw new RemoteException("Exception caught when restarting server", e);
        }

        String loggedInSessionCookie = getSessionCookie();
        eventReceiverAdminServiceClient = configurationUtil.getEventReceiverAdminServiceClient(
                backendURL, loggedInSessionCookie);
        eventStreamManagerAdminServiceClient = configurationUtil.getEventStreamManagerAdminServiceClient(
                backendURL, loggedInSessionCookie);
        eventPublisherAdminServiceClient = configurationUtil.getEventPublisherAdminServiceClient(
                backendURL, loggedInSessionCookie);
        Thread.sleep(45000);
    }



    @Test(groups = {"wso2.cep"},
            description = "Testing activemq jms receiver with Map formatted event with default mapping.")
    public void jmsMapTestWithDefaultMappingScenario() throws Exception {
        final int messageCount = 3;
        String samplePath = "inputflows" + File.separator + "sample0009";
        int startESCount = eventStreamManagerAdminServiceClient.getEventStreamCount();
        int startERCount = eventReceiverAdminServiceClient.getActiveEventReceiverCount();
        int startEPCount = eventPublisherAdminServiceClient.getActiveEventPublisherCount();

        // Add StreamDefinition.
        String streamDefinitionAsString = getJSONArtifactConfiguration(samplePath, SENSOR_STREAM_JSON);
        eventStreamManagerAdminServiceClient.addEventStreamAsString(streamDefinitionAsString);
        Assert.assertEquals(eventStreamManagerAdminServiceClient.getEventStreamCount(), startESCount + 1);

        // Add JMS Map EventReceiver without mapping.
        String eventReceiverConfig = getXMLArtifactConfiguration(samplePath, JMS_RECEIVER_MAP);
        eventReceiverAdminServiceClient.addEventReceiverConfiguration(eventReceiverConfig);
        Assert.assertEquals(eventReceiverAdminServiceClient.getActiveEventReceiverCount(), startERCount + 1);

        // Add Wso2event EventPublisher.
        String eventPublisherConfig = getXMLArtifactConfiguration(samplePath, WSO2_EVENT_PUBLISHER);
        eventPublisherAdminServiceClient.addEventPublisherConfiguration(eventPublisherConfig);
        Assert.assertEquals(eventPublisherAdminServiceClient.getActiveEventPublisherCount(), startEPCount + 1);

        // The data-bridge receiver.
        Wso2EventServer agentServer = new Wso2EventServer(samplePath, CEPIntegrationTestConstants.TCP_PORT, true);
        Thread agentServerThread = new Thread(agentServer);
        agentServerThread.start();

        // Wait for server to start and publish.
        Thread.sleep(5000);
        JMSPublisherClient.publish("topicMap", "csv", samplePath, "topicMap.csv");

        // Wait while all stats are published.
        Thread.sleep(5000);

        eventStreamManagerAdminServiceClient.removeEventStream("org.wso2.event.sensor.stream", "1.0.0");
        eventReceiverAdminServiceClient.removeInactiveEventReceiverConfiguration(JMS_RECEIVER_MAP);
        eventPublisherAdminServiceClient.removeInactiveEventPublisherConfiguration(WSO2_EVENT_PUBLISHER);

        Thread.sleep(2000);

        List<Event> eventList = new ArrayList<>();
        Event event = new Event();
        event.setStreamId(SENSOR_STREAM);
        event.setMetaData(new Object[]{19900813115534l, false, 601, "temperature"});
        event.setCorrelationData(new Object[]{90.34344, 20.44345});
        event.setPayloadData(new Object[]{2.3f, 20.44345});
        eventList.add(event);
        Event event2 = new Event();
        event2.setStreamId(SENSOR_STREAM);
        event2.setMetaData(new Object[]{19900813115534l, false, 602, "temperature"});
        event2.setCorrelationData(new Object[]{90.34344, 20.44345});
        event2.setPayloadData(new Object[]{2.3f, 20.44345});
        eventList.add(event2);
        Event event3 = new Event();
        event3.setStreamId(SENSOR_STREAM);
        event3.setMetaData(new Object[]{19900813115534l, false, 603, "temperature"});
        event3.setCorrelationData(new Object[]{90.34344, 20.44345});
        event3.setPayloadData(new Object[]{2.3f, 20.44345});
        eventList.add(event3);
        try {
            Assert.assertEquals(agentServer.getMsgCount(), messageCount, "Incorrect number of messages consumed!");
            List<Event> preservedEventList = agentServer.getPreservedEventList();
            for (Event aEvent : preservedEventList) {
                aEvent.setTimeStamp(0);
            }
            Assert.assertEquals(preservedEventList, eventList, "Mapping is incorrect!");
        } catch (Throwable e) {
            log.error("Exception thrown: " + e.getMessage(), e);
            Assert.fail("Exception: " + e.getMessage());
        } finally {
            agentServer.stop();
        }
    }

    @Test(groups = {"wso2.cep"},
            description = "Testing activemq jms receiver with Map formatted event with custom mapping",
            dependsOnMethods = {"jmsMapTestWithDefaultMappingScenario"})
    public void jmsMapTestWithCustomMappingScenario() throws Exception {
        final int messageCount = 3;
        String samplePath = "inputflows" + File.separator + "sample0010";
        int startESCount = eventStreamManagerAdminServiceClient.getEventStreamCount();
        int startERCount = eventReceiverAdminServiceClient.getActiveEventReceiverCount();
        int startEPCount = eventPublisherAdminServiceClient.getActiveEventPublisherCount();

        // Add StreamDefinition.
        String streamDefinitionAsString = getJSONArtifactConfiguration(samplePath,
                SENSOR_STREAM_JSON);
        eventStreamManagerAdminServiceClient.addEventStreamAsString(streamDefinitionAsString);
        Assert.assertEquals(eventStreamManagerAdminServiceClient.getEventStreamCount(), startESCount + 1);

        // Add JMS Map EventReceiver with mapping.
        String eventReceiverConfig = getXMLArtifactConfiguration(samplePath, JMS_RECEIVER_MAP);
        eventReceiverAdminServiceClient.addEventReceiverConfiguration(eventReceiverConfig);
        Assert.assertEquals(eventReceiverAdminServiceClient.getActiveEventReceiverCount(), startERCount + 1);

        // Add Wso2event EventPublisher.
        String eventPublisherConfig = getXMLArtifactConfiguration(samplePath, WSO2_EVENT_PUBLISHER);
        eventPublisherAdminServiceClient.addEventPublisherConfiguration(eventPublisherConfig);
        Assert.assertEquals(eventPublisherAdminServiceClient.getActiveEventPublisherCount(), startEPCount + 1);

        // The data-bridge receiver.
        Wso2EventServer agentServer = new Wso2EventServer(samplePath, CEPIntegrationTestConstants.TCP_PORT, true);
        Thread agentServerThread = new Thread(agentServer);
        agentServerThread.start();

        // Wait for server to start and publish.
        Thread.sleep(5000);
        JMSPublisherClient.publish("topicMap", "csv", samplePath, "topicMap.csv");

        // Wait while all stats are published.
        Thread.sleep(5000);

        eventStreamManagerAdminServiceClient.removeEventStream("org.wso2.event.sensor.stream", "1.0.0");
        eventReceiverAdminServiceClient.removeInactiveEventReceiverConfiguration(JMS_RECEIVER_MAP);
        eventPublisherAdminServiceClient.removeInactiveEventPublisherConfiguration(WSO2_EVENT_PUBLISHER);

        Thread.sleep(2000);

        List<Event> eventList = new ArrayList<>();
        Event event = new Event();
        event.setStreamId(SENSOR_STREAM);
        event.setMetaData(new Object[]{19900813115534l, false, 501, "temperature"});
        event.setCorrelationData(new Object[]{90.34344, 20.44345});
        event.setPayloadData(new Object[]{2.3f, 20.44345});
        eventList.add(event);
        Event event2 = new Event();
        event2.setStreamId(SENSOR_STREAM);
        event2.setMetaData(new Object[]{19900813115534l, false, 502, "temperature"});
        event2.setCorrelationData(new Object[]{90.34344, 20.44345});
        event2.setPayloadData(new Object[]{2.3f, 20.44345});
        eventList.add(event2);
        Event event3 = new Event();
        event3.setStreamId(SENSOR_STREAM);
        event3.setMetaData(new Object[]{19900813115534l, false, 503, "temperature"});
        event3.setCorrelationData(new Object[]{90.34344, 20.44345});
        event3.setPayloadData(new Object[]{2.3f, 20.44345});
        eventList.add(event3);
        try {
            Assert.assertEquals(agentServer.getMsgCount(), messageCount, "Incorrect number of messages consumed!");
            List<Event> preservedEventList = agentServer.getPreservedEventList();
            for (Event aEvent : preservedEventList) {
                aEvent.setTimeStamp(0);
            }
            Assert.assertEquals(preservedEventList, eventList, "Mapping is incorrect!");
        } catch (Throwable e) {
            log.error("Exception thrown: " + e.getMessage(), e);
            Assert.fail("Exception: " + e.getMessage());
        } finally {
            agentServer.stop();
        }
    }

    @Test(groups = {"wso2.cep"},
            description = "Testing activemq jms receiver with JSON formatted event with default mapping",
            dependsOnMethods = {"jmsMapTestWithCustomMappingScenario"})
    public void jmsJSONTestWithDefaultMappingScenario() throws Exception {
        final int messageCount = 3;
        String samplePath = "inputflows" + File.separator + "sample0011";
        int startESCount = eventStreamManagerAdminServiceClient.getEventStreamCount();
        int startERCount = eventReceiverAdminServiceClient.getActiveEventReceiverCount();
        int startEPCount = eventPublisherAdminServiceClient.getActiveEventPublisherCount();

        // Add StreamDefinition.
        String streamDefinitionAsString = getJSONArtifactConfiguration(samplePath,
                SENSOR_STREAM_JSON);
        eventStreamManagerAdminServiceClient.addEventStreamAsString(streamDefinitionAsString);
        Assert.assertEquals(eventStreamManagerAdminServiceClient.getEventStreamCount(), startESCount + 1);

        // Add JMS JSON EventReceiver without mapping.
        String eventReceiverConfig = getXMLArtifactConfiguration(samplePath, "jmsReceiverJSON.xml");
        eventReceiverAdminServiceClient.addEventReceiverConfiguration(eventReceiverConfig);
        Assert.assertEquals(eventReceiverAdminServiceClient.getActiveEventReceiverCount(), startERCount + 1);

        // Add Wso2event EventPublisher.
        String eventPublisherConfig = getXMLArtifactConfiguration(samplePath, WSO2_EVENT_PUBLISHER);
        eventPublisherAdminServiceClient.addEventPublisherConfiguration(eventPublisherConfig);
        Assert.assertEquals(eventPublisherAdminServiceClient.getActiveEventPublisherCount(), startEPCount + 1);

        // The data-bridge receiver.
        Wso2EventServer agentServer = new Wso2EventServer(samplePath, CEPIntegrationTestConstants.TCP_PORT, true);
        Thread agentServerThread = new Thread(agentServer);
        agentServerThread.start();

        // Wait for server to start and publish.
        Thread.sleep(5000);
        JMSPublisherClient.publish("topicJSON", "json", samplePath, "topicJSON.txt");

        // Wait while all stats are published.
        Thread.sleep(5000);

        eventStreamManagerAdminServiceClient.removeEventStream("org.wso2.event.sensor.stream", "1.0.0");
        eventReceiverAdminServiceClient.removeInactiveEventReceiverConfiguration("jmsReceiverJSON.xml");
        eventPublisherAdminServiceClient.removeInactiveEventPublisherConfiguration(WSO2_EVENT_PUBLISHER);

        Thread.sleep(2000);
        List<Event> eventList = new ArrayList<>();
        Event event = new Event();
        event.setStreamId(SENSOR_STREAM);
        event.setMetaData(new Object[]{19900813115534l, false, 701, "temperature"});
        event.setCorrelationData(new Object[]{90.34344, 20.44345});
        event.setPayloadData(new Object[]{2.3f, 20.44345});
        eventList.add(event);
        Event event2 = new Event();
        event2.setStreamId(SENSOR_STREAM);
        event2.setMetaData(new Object[]{19900813115534l, false, 702, "temperature"});
        event2.setCorrelationData(new Object[]{90.34344, 20.44345});
        event2.setPayloadData(new Object[]{2.3f, 20.44345});
        eventList.add(event2);
        Event event3 = new Event();
        event3.setStreamId(SENSOR_STREAM);
        event3.setMetaData(new Object[]{19900813115534l, false, 703, "temperature"});
        event3.setCorrelationData(new Object[]{90.34344, 20.44345});
        event3.setPayloadData(new Object[]{2.3f, 20.44345});
        eventList.add(event3);
        try {
            Assert.assertEquals(agentServer.getMsgCount(), messageCount, "Incorrect number of messages consumed!");
            List<Event> preservedEventList = agentServer.getPreservedEventList();
            for (Event aEvent : preservedEventList) {
                aEvent.setTimeStamp(0);
            }
            Assert.assertEquals(preservedEventList, eventList, "Mapping is incorrect!");
        } catch (Throwable e) {
            log.error("Exception thrown: " + e.getMessage(), e);
            Assert.fail("Exception: " + e.getMessage());
        } finally {
            agentServer.stop();
        }
    }

    @Test(groups = {"wso2.cep"},
            description = "Testing activemq jms receiver with XML formatted event with default mapping",
            dependsOnMethods = {"jmsJSONTestWithDefaultMappingScenario"})
    public void jmsXmlTestWithDefaultMappingScenario() throws Exception {
        final int messageCount = 2;
        String samplePath = "inputflows" + File.separator + "sample0011";
        int startESCount = eventStreamManagerAdminServiceClient.getEventStreamCount();
        int startERCount = eventReceiverAdminServiceClient.getActiveEventReceiverCount();
        int startEPCount = eventPublisherAdminServiceClient.getActiveEventPublisherCount();

        // Add StreamDefinition.
        String streamDefinitionAsString = getJSONArtifactConfiguration(samplePath,
                SENSOR_STREAM_JSON);
        eventStreamManagerAdminServiceClient.addEventStreamAsString(streamDefinitionAsString);
        Assert.assertEquals(eventStreamManagerAdminServiceClient.getEventStreamCount(), startESCount + 1);

        // Add JMS XML EventReceiver without mapping.
        String eventReceiverConfig2 = getXMLArtifactConfiguration(samplePath, "jmsReceiverXML.xml");
        eventReceiverAdminServiceClient.addEventReceiverConfiguration(eventReceiverConfig2);
        Assert.assertEquals(eventReceiverAdminServiceClient.getActiveEventReceiverCount(), startERCount + 1);

        // Add Wso2event EventPublisher.
        String eventPublisherConfig = getXMLArtifactConfiguration(samplePath, WSO2_EVENT_PUBLISHER);
        eventPublisherAdminServiceClient.addEventPublisherConfiguration(eventPublisherConfig);
        Assert.assertEquals(eventPublisherAdminServiceClient.getActiveEventPublisherCount(), startEPCount + 1);

        // The data-bridge receiver.
        Wso2EventServer agentServer = new Wso2EventServer(samplePath, CEPIntegrationTestConstants.TCP_PORT, true);
        Thread agentServerThread = new Thread(agentServer);
        agentServerThread.start();

        // Wait for server to start and publish.
        Thread.sleep(5000);
        JMSPublisherClient.publish("topicXML", "xml", samplePath, "topicXML.txt");

        // Wait while all stats are published.
        Thread.sleep(2000);

        eventStreamManagerAdminServiceClient.removeEventStream("org.wso2.event.sensor.stream", "1.0.0");
        eventReceiverAdminServiceClient.removeInactiveEventReceiverConfiguration("jmsReceiverXML.xml");
        eventPublisherAdminServiceClient.removeInactiveEventPublisherConfiguration(WSO2_EVENT_PUBLISHER);

        Thread.sleep(2000);
        List<Event> eventList = new ArrayList<>();
        Event event4 = new Event();
        event4.setStreamId(SENSOR_STREAM);
        event4.setMetaData(new Object[]{199008131245l, true, 801, "temperature"});
        event4.setCorrelationData(new Object[]{4.504343, 1.23434});
        event4.setPayloadData(new Object[]{6.6f, 20.44345});
        eventList.add(event4);
        Event event5 = new Event();
        event5.setStreamId(SENSOR_STREAM);
        event5.setMetaData(new Object[]{199008131245l, true, 802, "temperature"});
        event5.setCorrelationData(new Object[]{4.504343, 1.23434});
        event5.setPayloadData(new Object[]{6.6f, 20.44345});
        eventList.add(event5);
        try {
            Assert.assertEquals(agentServer.getMsgCount(), messageCount, "Incorrect number of messages consumed!");
            List<Event> preservedEventList = agentServer.getPreservedEventList();
            for (Event aEvent : preservedEventList) {
                aEvent.setTimeStamp(0);
            }
            Assert.assertEquals(preservedEventList, eventList, "Mapping is incorrect!");
        } catch (Throwable e) {
            log.error("Exception thrown: " + e.getMessage(), e);
            Assert.fail("Exception: " + e.getMessage());
        } finally {
            agentServer.stop();
        }
    }

    @Test(groups = {"wso2.cep"},
            description = "Testing activemq jms receiver with JSON formatted event with default mapping",
            dependsOnMethods = {"jmsXmlTestWithDefaultMappingScenario"})
    public void jmsTextTestWithDefaultMappingScenario() throws Exception {
        final int messageCount = 3;
        String samplePath = "inputflows" + File.separator + "sample0011";
        int startESCount = eventStreamManagerAdminServiceClient.getEventStreamCount();
        int startERCount = eventReceiverAdminServiceClient.getActiveEventReceiverCount();
        int startEPCount = eventPublisherAdminServiceClient.getActiveEventPublisherCount();

        // Add StreamDefinition.
        String streamDefinitionAsString = getJSONArtifactConfiguration(samplePath,
                SENSOR_STREAM_JSON);
        eventStreamManagerAdminServiceClient.addEventStreamAsString(streamDefinitionAsString);
        Assert.assertEquals(eventStreamManagerAdminServiceClient.getEventStreamCount(), startESCount + 1);

        // Add JMS Text EventReceiver without mapping.
        String eventReceiverConfig3 = getXMLArtifactConfiguration(samplePath, "jmsReceiverText.xml");
        eventReceiverAdminServiceClient.addEventReceiverConfiguration(eventReceiverConfig3);
        Assert.assertEquals(eventReceiverAdminServiceClient.getActiveEventReceiverCount(), startERCount + 1);

        // Add Wso2event EventPublisher.
        String eventPublisherConfig = getXMLArtifactConfiguration(samplePath, WSO2_EVENT_PUBLISHER);
        eventPublisherAdminServiceClient.addEventPublisherConfiguration(eventPublisherConfig);
        Assert.assertEquals(eventPublisherAdminServiceClient.getActiveEventPublisherCount(), startEPCount + 1);

        // The data-bridge receiver.
        Wso2EventServer agentServer = new Wso2EventServer(samplePath, CEPIntegrationTestConstants.TCP_PORT, true);
        Thread agentServerThread = new Thread(agentServer);
        agentServerThread.start();

        // Wait for server to start and publish.
        Thread.sleep(5000);
        JMSPublisherClient.publish("topicText", "text", samplePath, "topicText.txt");

        // Wait while all stats are published.
        Thread.sleep(2000);

        eventStreamManagerAdminServiceClient.removeEventStream("org.wso2.event.sensor.stream", "1.0.0");
        eventReceiverAdminServiceClient.removeInactiveEventReceiverConfiguration("jmsReceiverText.xml");
        eventPublisherAdminServiceClient.removeInactiveEventPublisherConfiguration(WSO2_EVENT_PUBLISHER);

        Thread.sleep(2000);
        List<Event> eventList = new ArrayList<>();
        Event event6 = new Event();
        event6.setStreamId(SENSOR_STREAM);
        event6.setMetaData(new Object[]{19900813115534l, false, 901, "temperature"});
        event6.setCorrelationData(new Object[]{20.44345, 5.443435});
        event6.setPayloadData(new Object[]{8.9f, 1.23434});
        eventList.add(event6);
        Event event7 = new Event();
        event7.setStreamId(SENSOR_STREAM);
        event7.setMetaData(new Object[]{19900813115534l, false, 902, "temperature"});
        event7.setCorrelationData(new Object[]{20.44345, 5.443435});
        event7.setPayloadData(new Object[]{8.9f, 1.23434});
        eventList.add(event7);
        Event event8 = new Event();
        event8.setStreamId(SENSOR_STREAM);
        event8.setMetaData(new Object[]{19900813115534l, false, 903, "temperature"});
        event8.setCorrelationData(new Object[]{20.44345, 5.443435});
        event8.setPayloadData(new Object[]{8.9f, 1.23434});
        eventList.add(event8);
        try {
            Assert.assertEquals(agentServer.getMsgCount(), messageCount, "Incorrect number of messages consumed!");
            List<Event> preservedEventList = agentServer.getPreservedEventList();
            for (Event aEvent : preservedEventList) {
                aEvent.setTimeStamp(0);
            }
            Assert.assertEquals(preservedEventList, eventList, "Mapping is incorrect!");
        } catch (Throwable e) {
            log.error("Exception thrown: " + e.getMessage(), e);
            Assert.fail("Exception: " + e.getMessage());
        } finally {
            agentServer.stop();
        }
    }


    @Test(groups = {"wso2.cep"}, description = "Testing jms receiver with jms properties",
            dependsOnMethods = {"jmsTextTestWithDefaultMappingScenario"})
    public void jmsPropertiesTestWithDefaultMappingScenario() throws Exception {
        final int messageCount = 3;
        String samplePath = "inputflows" + File.separator + "sample0022";
        int startESCount = eventStreamManagerAdminServiceClient.getEventStreamCount();
        int startERCount = eventReceiverAdminServiceClient.getActiveEventReceiverCount();
        int startEPCount = eventPublisherAdminServiceClient.getActiveEventPublisherCount();

        // Add StreamDefinition.
        String streamDefinitionAsString = getJSONArtifactConfiguration(samplePath,
                SENSOR_STREAM_JSON);
        eventStreamManagerAdminServiceClient.addEventStreamAsString(streamDefinitionAsString);
        Assert.assertEquals(eventStreamManagerAdminServiceClient.getEventStreamCount(), startESCount + 1);

        // Add JMS Map EventReceiver.
        String eventReceiverConfig = getXMLArtifactConfiguration(samplePath, "jmsReceiver.xml");
        eventReceiverAdminServiceClient.addEventReceiverConfiguration(eventReceiverConfig);
        Assert.assertEquals(eventReceiverAdminServiceClient.getActiveEventReceiverCount(), startERCount + 1);

        // Add Wso2event EventPublisher.
        String eventPublisherConfig = getXMLArtifactConfiguration(samplePath, WSO2_EVENT_PUBLISHER);
        eventPublisherAdminServiceClient.addEventPublisherConfiguration(eventPublisherConfig);
        Assert.assertEquals(eventPublisherAdminServiceClient.getActiveEventPublisherCount(), startEPCount + 1);

        // The data-bridge receiver.
        Wso2EventServer agentServer = new Wso2EventServer(samplePath, CEPIntegrationTestConstants.TCP_PORT, true);
        Thread agentServerThread = new Thread(agentServer);
        try {
            // Let the server start.
            agentServerThread.start();
            Thread.sleep(5000);
        } catch (Throwable t) {
            // Just to diagnose.
            log.error("Could not start the agent server ", t);
        }

        // Edit receiver by adding JMS properties.
        String eventReceiverNewConfig = getXMLArtifactConfiguration(samplePath, "jmsPropertiesReceiver.xml");
        eventReceiverAdminServiceClient.editEventReceiverConfiguration(eventReceiverNewConfig, "jmsReceiver");
        Assert.assertEquals(eventReceiverAdminServiceClient.getActiveEventReceiverCount(), startERCount + 1);

        // Wait for server to start and then publish.
        Thread.sleep(10000);
        JMSPublisherClient.publish("topicMap", "csv", samplePath, "topicMap.csv");

        // Wait while all stats are published.
        Thread.sleep(5000);
        eventStreamManagerAdminServiceClient.removeEventStream("org.wso2.event.sensor.stream", "1.0.0");
        eventReceiverAdminServiceClient.removeInactiveEventReceiverConfiguration("jmsReceiver.xml");
        eventPublisherAdminServiceClient.removeInactiveEventPublisherConfiguration(WSO2_EVENT_PUBLISHER);

        Thread.sleep(2000);
        List<Event> eventList = new ArrayList<>();
        Event event = new Event();
        event.setStreamId(SENSOR_STREAM);
        event.setMetaData(new Object[]{19900813115534l, false, 601, "temperature"});
        event.setCorrelationData(new Object[]{90.34344, 20.44345});
        event.setPayloadData(new Object[]{2.3f, 20.44345});
        eventList.add(event);
        Event event2 = new Event();
        event2.setStreamId(SENSOR_STREAM);
        event2.setMetaData(new Object[]{19900813115534l, false, 602, "temperature"});
        event2.setCorrelationData(new Object[]{90.34344, 20.44345});
        event2.setPayloadData(new Object[]{2.3f, 20.44345});
        eventList.add(event2);
        Event event3 = new Event();
        event3.setStreamId(SENSOR_STREAM);
        event3.setMetaData(new Object[]{19900813115534l, false, 603, "temperature"});
        event3.setCorrelationData(new Object[]{90.34344, 20.44345});
        event3.setPayloadData(new Object[]{2.3f, 20.44345});
        eventList.add(event3);
        try {
            Assert.assertEquals(agentServer.getMsgCount(), messageCount, "Incorrect number of messages consumed!");
            List<Event> preservedEventList = agentServer.getPreservedEventList();
            for (Event aEvent : preservedEventList) {
                aEvent.setTimeStamp(0);
            }
            Assert.assertEquals(preservedEventList, eventList, "Mapping is incorrect!");
        } catch (Throwable e) {
            log.error("Exception thrown: " + e.getMessage(), e);
            Assert.fail("Exception: " + e.getMessage());
        } finally {
            try {
                agentServer.stop();
            } catch (Throwable t) {
                // Just to diagnose.
                log.error("Could not stop the agent server ", t);
            }
        }
    }


    @Test(groups = {"wso2.cep"},
            description = "Testing activemq jms receiver with waiting time. Due to large wating time lisnter should " +
                    "not consume any messages from ActiveMQ",
        dependsOnMethods = {"jmsPropertiesTestWithDefaultMappingScenario"})
    public void jmsMapTestWithWaitingTime() throws Exception {
        String samplePath = "inputflows" + File.separator + "sample0009";

        String configFilePath = getTestArtifactLocation() + CEPIntegrationTestConstants
                .RELATIVE_PATH_TO_TEST_ARTIFACTS + samplePath + File.separator + "input-event-adapters.xml";
        configFilePath = configFilePath.replaceAll("[\\\\/]", Matcher.quoteReplacement(File.separator));

        File configFile = new File(configFilePath);
        File targetConfigFile = new File(serverManager.getCarbonHome() + File.separator + "repository" + File
                .separator + "conf" + File.separator  + "input-event-adapters.xml");
        serverManager.applyConfiguration(configFile, targetConfigFile);

        String loggedInSessionCookie = getSessionCookie();
        eventReceiverAdminServiceClient = configurationUtil.getEventReceiverAdminServiceClient(
                backendURL, loggedInSessionCookie);
        eventStreamManagerAdminServiceClient = configurationUtil.getEventStreamManagerAdminServiceClient(
                backendURL, loggedInSessionCookie);
        eventPublisherAdminServiceClient = configurationUtil.getEventPublisherAdminServiceClient(
                backendURL, loggedInSessionCookie);
        Thread.sleep(45000);

        int startESCount = eventStreamManagerAdminServiceClient.getEventStreamCount();
        int startERCount = eventReceiverAdminServiceClient.getActiveEventReceiverCount();
        int startEPCount = eventPublisherAdminServiceClient.getActiveEventPublisherCount();

        // Add StreamDefinition.
        String streamDefinitionAsString = getJSONArtifactConfiguration(samplePath,
                SENSOR_STREAM_JSON);
        eventStreamManagerAdminServiceClient.addEventStreamAsString(streamDefinitionAsString);
        Assert.assertEquals(eventStreamManagerAdminServiceClient.getEventStreamCount(), startESCount + 1);

        // Add JMS Map EventReceiver without mapping.
        String eventReceiverConfig = getXMLArtifactConfiguration(samplePath, JMS_RECEIVER_MAP);
        eventReceiverAdminServiceClient.addEventReceiverConfiguration(eventReceiverConfig);
        Assert.assertEquals(eventReceiverAdminServiceClient.getActiveEventReceiverCount(), startERCount + 1);

        // Add Wso2event EventPublisher.
        String eventPublisherConfig = getXMLArtifactConfiguration(samplePath, WSO2_EVENT_PUBLISHER);
        eventPublisherAdminServiceClient.addEventPublisherConfiguration(eventPublisherConfig);
        Assert.assertEquals(eventPublisherAdminServiceClient.getActiveEventPublisherCount(), startEPCount + 1);

        // The data-bridge receiver.
        Wso2EventServer agentServer = new Wso2EventServer(samplePath, CEPIntegrationTestConstants.TCP_PORT, true);
        Thread agentServerThread = new Thread(agentServer);
        agentServerThread.start();

        // Wait for server to start and publish.
        Thread.sleep(5000);
        JMSPublisherClient.publish("topicMap", "csv", samplePath, "topicMap.csv");

        // Wait while all stats are published.
        Thread.sleep(5000);

        eventStreamManagerAdminServiceClient.removeEventStream("org.wso2.event.sensor.stream", "1.0.0");
        eventReceiverAdminServiceClient.removeInactiveEventReceiverConfiguration(JMS_RECEIVER_MAP);
        eventPublisherAdminServiceClient.removeInactiveEventPublisherConfiguration(WSO2_EVENT_PUBLISHER);

        Thread.sleep(2000);

        List<Event> eventList = new ArrayList<>();
        Event event = new Event();
        event.setStreamId(SENSOR_STREAM);
        event.setMetaData(new Object[]{19900813115534l, false, 601, "temperature"});
        event.setCorrelationData(new Object[]{90.34344, 20.44345});
        event.setPayloadData(new Object[]{2.3f, 20.44345});
        eventList.add(event);

        try {
            // No messages should be received due to large listener waiting time
            Assert.assertEquals(agentServer.getMsgCount(), 0, "Messages consumed by listener!!.");
        } catch (Throwable e) {
            log.error("Exception thrown: " + e.getMessage(), e);
            Assert.fail("Exception: " + e.getMessage());
        } finally {
            agentServer.stop();
        }
    }

    @AfterClass(alwaysRun = true)
    public void destroy() throws Exception {
        try {
            Thread.sleep(5000);
            if (activeMqBroker != null) {
                activeMqBroker.stop();
            }
            // Wait for server to clear the artifact un-deployment.
            Thread.sleep(5000);
        } finally {
            // Revert the changes done to cep sever.
            if (serverManager != null) {
                serverManager.removeFromComponentLib(ACTIVEMQ_CORE);
                serverManager.removeFromComponentLib(GERONIMO_J2EE_MANAGEMENT);
                serverManager.restoreToLastConfiguration();
            }
        }
        super.cleanup();
    }

    private void setupActiveMQBroker() {
        activeMqBroker = new JMSBrokerController("localhost", getJMSBrokerConfiguration());
        if (!JMSBrokerController.isBrokerStarted()) {
            Assert.assertTrue(activeMqBroker.start(), "ActiveMQ Broker starting failed");
        }
    }

    private JMSBrokerConfiguration getJMSBrokerConfiguration() {
        return JMSBrokerConfigurationProvider.getInstance().getBrokerConfiguration();
    }
}