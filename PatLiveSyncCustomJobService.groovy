import groovy.time.TimeCategory
import java.sql.SQLException
import groovy.sql.*
import com.mphrx.base.BaseService
import com.mphrx.dicr.JobConfiguration
import org.apache.log4j.Logger
import org.codehaus.groovy.grails.commons.GrailsApplication
import com.mongodb.*
import com.mphrx.dicr.DicrConfig
import grails.util.Holders
import java.text.SimpleDateFormat

public class PatLiveSyncCustomJobTestingService extends BaseService {
    Logger log = Logger.getLogger(PatLiveSyncCustomJobTestingService.class)
    GrailsApplication grailsApplication
    def medantaCommonUtilitiesService = Holders.grailsApplication.mainContext.getBean("medantaCommonUtilitiesService")
    
    def today
    def currentDate = new Date()
    String startDateForQuery = ""
    String endDateForQuery = ""
    String patientIdForQuery = ""
    def pattern = "yyyy-MM-dd HH:mm:ss"

    @Override
    def executeService(JobConfiguration jobConfiguration) {
        serviceName = "patLiveSyncCustomJobTestingService"
        log.info("The Service [$serviceName] Started at [$currentDate] where Job Config Id is  [$jobConfiguration.id]")
        if (!changeCurrentJobForMultipleInstance(1,jobConfiguration.modInstance)) {
            log.info("The current job [$jobConfiguration] already running")
            return
        }

        try {
            log.info("[Message: ${serviceName}]: Importing")
            patLivSyncData(jobConfiguration)
            currentDate = new Date()
            log.info("[Message: ${serviceName}]: End Time: ${currentDate}")
        } catch (Exception ex) {
            log.error("Error: Exception occurred to process ${serviceName} custom job ", ex)
        } finally {
            changeCurrentJobForMultipleInstance(-1,jobConfiguration.modInstance)
        }
    }

    //Checking Values Either they are in favour or not
    def patLivSyncData(JobConfiguration jobConfiguration) {
        def startTime = new Date()
        startDateForQuery = ""
        endDateForQuery = ""
        patientIdForQuery = ""
        def jobEnableFlag = "true"
        boolean enableJob = false

        SimpleDateFormat formatter = new SimpleDateFormat(pattern)

        String queryCondition = " and  P.MODIFIED_DATE >= SYSDATE - 1"

        try {
            DicrConfig startDate = DicrConfig.findByParamName("jobno_${jobConfiguration.id}_startDate")
            if (startDate == null || startDate.paramValue == "" || startDate.paramValue == null) {
                log.info("[INFO: ${serviceName}]: Start Date for query with param [JobNo_${jobConfiguration.id}_startDate] is not configured.")
            } else {
                startDateForQuery = startDate.paramValue?.trim()
                log.info "[INFO: ${serviceName}]: Start Date for query with param [JobNo_${jobConfiguration.id}_startDate] is [${startDateForQuery}]"
            }

            DicrConfig endDate = DicrConfig.findByParamName("jobno_${jobConfiguration.id}_endDate")
            if (endDate == null || endDate.paramValue == "" || endDate.paramValue == null) {
                log.info("[INFO: ${serviceName}: End date is not available with param [jobno_${jobConfiguration.id}_endDate] is not configured]")
            } else {
                endDateForQuery = endDate.paramValue?.trim()
                log.info("[INFO: ${serviceName}: End date with param [jobno_${jobConfiguration.id}_endDate] is ${endDateForQuery}]")
            }

            DicrConfig jobEnable = DicrConfig.findByParamName("jobno_${jobConfiguration.id}_jobEnable")
            if (jobEnable == null || jobEnable.paramValue == "" || jobEnable.paramValue == null) {
                log.info("[INFO: ${serviceName}: job enable with the param [jobno_${jobConfiguration.id}_jobEnable] is not configured]")
            } else {
                jobEnableFlag = jobEnable.paramValue?.trim()
                log.info("[INFO: ${serviceName}: job enable with the param [jobno_${jobConfiguration.id}_jobEnable] is ${jobEnableFlag}")
            }

            DicrConfig patientId = DicrConfig.findByParamName("jobno_${jobConfiguration.id}_patientId")
            if (patientId == null || patientId.paramValue == null || patientId.paramValue == "") {
                log.info("[INFO: ${serviceName}: job enable with the param [jobno_${jobConfiguration.id}_patientId] is not configred")
            } else {
                patientIdForQuery = patientId.paramValue?.trim()
                log.info("\"[INFO: ${serviceName}: job enable with the param [jobno_${jobConfiguration.id}_patientId] is ${patientIdForQuery}")
            }

            if ((startDateForQuery && startDateForQuery != "NA" && endDateForQuery && endDateForQuery != "NA" && (jobEnableFlag == "true")))
            {
                def timeStart =startDateForQuery
                def timeEnd =endDateForQuery

                queryCondition = """ and P.MODIFIED_DATE between to_date('${timeStart}', 'YYYY-MM-DD HH24:MI:SS') and 
                                            to_date('${timeEnd}', 'YYYY-MM-DD HH24:MI:SS')"""
            }
            else if(((patientIdForQuery && patientIdForQuery != "" && patientIdForQuery != "NA") && (jobEnableFlag == "true")))
            {
                queryCondition = """  and P.patient_id = '${patientIdForQuery}'"""
            }
            else
            {
                def lastProccessTime = medantaCommonUtilitiesService.GetJobLastRunTime(serviceName,jobConfiguration.modInstance)
                if(lastProccessTime && (lastProccessTime != "" || lastProccessTime != "NA"))
                {
                    queryCondition = """ and  P.MODIFIED_DATE >= to_date('${lastProccessTime}','YYYY-MM-DD HH24:MI:SS')"""
                }
            }
            def query = medantaCommonUtilitiesService.patLivSyncBaseQuery() + queryCondition
	    log.info("[Info : '${serviceName}' has query '${query}']")
            def currTime = new Date()
            def documentCount = medantaCommonUtilitiesService.dumpPatLivSyncData(query)
            Map countMap = [:]
            if(documentCount >= 0) {
                log.info("your document has been dumped successfully, number of documents are '${documentCount}'")
                countMap.put("documentCount", documentCount)
                def sucFlag = medantaCommonUtilitiesService.updateCustomJobLog(countMap, serviceName, currTime,jobConfiguration.modInstance)
                if (sucFlag) {
                    log.info("Document Map updated Successfully>>>>>>>>>")
                }
		if(jobEnable){
			jobEnable.paramValue = "false"
			jobEnable.save()
		}
            }
        } catch (Exception ex) {
            log.error("Error: Exception occurred to process ${serviceName} Some error occured ", ex)
        }
    }
}
