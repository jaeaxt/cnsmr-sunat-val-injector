package com.axt.cnsmr.sunat.val.injector;

import com.axteroid.sdk.documents.DocumentInfoCtlr;
import com.axteroid.sdk.documents.DocumentInfoDataIndex;
import com.axteroid.sdk.documents.DocumentInfoDataStorage;
import com.axteroid.sdk.documents.DocumentInfoFileStorage;
import com.axteroid.sdk.documents.impl.DocumentInfoDataStorageFactory;
import com.axteroid.sdk.documents.impl.DocumentInfoFileStorageFactory;
import com.axteroid.sdk.documents.model.*;
import com.axteroid.sdk.queuing.model.AgencySendMessage;
import com.axteroid.sdk.queuing.producers.DocumentEventProducer;
import com.axteroid.sdk.queuing.producers.SunatValProducer;
import com.axteroid.sdk.utils.Version;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Deque;
import java.util.EnumSet;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.axteroid.sdk.exceptions.NotFoundException.notFoundException;
import static com.axteroid.sdk.utils.Environment.AxtVar.KAFKA_BOOTSTRAP_SERVERS;
import static com.axteroid.sdk.utils.Util.*;

public class Injector {

    private static final Logger LOGGER = LogManager.getLogger(Injector.class);
    private static final String BOOTSTRAP_SERVERS = KAFKA_BOOTSTRAP_SERVERS.getValue();

    public static void main(String[] args) throws Exception {
        System.out.println("Axt SDK version: " + Version.getSdkVersion());


        LOGGER.info("=== AXT Kafka Avro Producer arrancando ===");

        DocumentInfoDataIndex dataIndex = new ExtDocumentInfoOpSearchDI();
        DocumentInfoDataStorage dataStorage = DocumentInfoDataStorageFactory.getImplementation();
        DocumentInfoFileStorage fileStorage = DocumentInfoFileStorageFactory.getImplementation();
        DocumentEventProducer eventProducer = new DocumentEventProducer(BOOTSTRAP_SERVERS);

        var documentInfoCtlr = new DocumentInfoCtlr(dataStorage, dataIndex, null, fileStorage, null);
        SunatValProducer producer = new SunatValProducer(BOOTSTRAP_SERVERS);
        if (args.length > 0) {
            if (args.length != 2) {
                System.out.println("You must provide accId and docId");
                System.exit(-1);
            }

            String accId = args[0];
            String docId = args[1];
            LOGGER.info("rencolando documento {} : {}", docId, accId);
            DocumentInfo documentInfo = documentInfoCtlr.getById(accId, new DocumentId(docId)).orElseThrow(notFoundException("accId: " + accId + " docId: " + docId + " not found"));
//            AgencySendMessage message = new AgencySendMessage(documentInfo);
            injectDocument(documentInfo, documentInfoCtlr, producer);
            LOGGER.info("Mensaje enviado exitosamente.");
        } else {
            Instant startTime = Instant.parse("2026-04-16T00:00:00Z");
            Instant endTime = Instant.parse("2026-04-17T00:00:00Z");

            Map<String, Deque<String>> params = new WeakHashMap<>();
            params.put("test", dequeFromBooleans(false));
            params.put("page_size", dequeFromIntegers(10000));
            params.put("created__gte", dequeFromInstants(startTime));
            params.put("created__lt", dequeFromInstants(endTime));
            params.put("status", dequeFromEnumSet(EnumSet.of(DocumentStatus.APR, DocumentStatus.OBS)));
            params.put("country", dequeFromStrings("PE"));
            params.put("type", dequeFromEnumSet(EnumSet.of(DocumentType.PE01, DocumentType.PE03, DocumentType.PE07, DocumentType.PE08)));
            params.put("tax_authority_send_status__not_exists", dequeFromStrings("tax_authority_send_status"));

            Long countResponse = dataIndex.getCount(params);
            LOGGER.info("Cantidad de Elementos: " + countResponse);
            AtomicLong result = new AtomicLong();
            if (countResponse < 10000) {
                LOGGER.info("Ejecutando 1 Query: " + countResponse);
                documentInfoCtlr.globalQuery(params).forEach(doc -> {
//                injectDocument(doc, documentInfoCtlr, producer);
                    LOGGER.info("{} - accId {} - docId {}", result.getAndIncrement(), doc.accId, doc.id);
                });
                LOGGER.info("Total resultados de la query: {}", result.get());
            } else {
                LOGGER.info("Separando Rango de fecha en tramos de 15 minutos");
                Instant lastTime = startTime.plus(15, ChronoUnit.MINUTES);
                params.remove("created__lt");
                params.put("created__lt", dequeFromInstants(lastTime));
                long cantResultados = 0L;
                do {
                    LOGGER.info(startTime.toString() + " - " + lastTime.toString());
                    documentInfoCtlr.globalQuery(params).forEach(doc -> {
//                    injectDocument(doc, documentInfoCtlr, producer);
                        LOGGER.info("{} - accId {} - docId {}", result.getAndIncrement(), doc.accId, doc.id);
                    });
                    LOGGER.info("Total resultados de la query: {}", result.get());
                    cantResultados += result.get();
                    result.set(0L);
                    LOGGER.info("TOTAL DE RESULTADO : {}", cantResultados);
                    startTime = lastTime;
                    lastTime = startTime.plus(15, ChronoUnit.MINUTES);
                    params.put("created__gte", dequeFromInstants(startTime));
                    params.put("created__lt", dequeFromInstants(lastTime));
                    params.put("page_size", dequeFromIntegers(10000));
                } while (endTime.isAfter(lastTime));

            }
        }
        // producer.produce(message);
//        LOGGER.info("Mensaje enviado exitosamente.");
        producer.close();
        LOGGER.info("=== Producer finalizado ===");
        dataIndex.close();
        LOGGER.info("=== Conexion OpenSearch cerrada ===");
    }

    private static void injectDocument(DocumentInfo doc, DocumentInfoCtlr ctlr, SunatValProducer producer) {
        doc.taxAuthoritySendStatus = TaxAuthoritySendStatus.MRT;
        ctlr.updateTaxAuthoritySendStatus(doc);
        producer.produce(new AgencySendMessage(doc));
    }
}
