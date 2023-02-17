/**
 * Created by Garima on 08/11/2017.
 */
import grails.transaction.Transactional
import groovy.util.slurpersupport.NodeChild

import java.sql.SQLException
import java.text.SimpleDateFormat
import groovy.sql.Sql
import org.apache.log4j.Logger
import org.codehaus.groovy.grails.commons.GrailsApplication
import org.springframework.beans.factory.DisposableBean
import org.springframework.beans.factory.InitializingBean
import com.mongodb.util.JSON
import groovy.json.JsonOutput
import com.mongodb.*
import org.wkhtmltox.WkhtmltoxWrapper
import org.wkhtmltox.WkhtmltoxExecutor
import com.mphrx.util.calendar.DateUtils
import com.mphrx.util.grails.ApplicationContextUtil

@Transactional
public class  MedantaCommonUtilitiesService implements InitializingBean, DisposableBean
{
    public static Logger log = Logger.getLogger("com.mphrx.MedantaCommonUtilitiesService")
	def configObject = ApplicationContextUtil.getConfig();
    GrailsApplication grailsApplication
    MongoClient mongoClient
    DB db
    String serviceName ="";
    def xmlParserService  = ApplicationContextUtil.getBeanByName("xmlParserService")
	public static String custId = System.getenv("CUSTID")
	public static String cssPath = "/opt/mphrx/" + custId + "/data/themes/assets/"
	public String defaultPractitionerSignLocation = configObject.customJobService.defaultPractitionerSignLocation ?: "/opt/mphrx/" + custId + "/data/practitioner_signature/"
    public def labOrmQuery(String QueryDate,String serviceNameOrg,Map driverMap,boolean isReconJob = false)
    {
        int counter = 0
        Boolean isSuccess = false
	    String collectionName ="labOrmReportDump"
        serviceName = serviceNameOrg;
	log.error("Inside labOrmQuery method with reconJob: ${isReconJob}")
        try {
	    List testCodeArr = [];
	    int record_count = 0;
	    (testCodeArr,record_count) = checkSensitivityTestCode(serviceName,driverMap);
	    Map microbiologyTestHash = organismTestCodeHash(serviceName);
            log.error("TESTCODEHASH: ========== "+ microbiologyTestHash);
            //if(organismTestCode[test])
            //log.error("TESTCODE: ==========${test} ");

	    def instance = driverMap.instance
            def user = driverMap.user
            def password = driverMap.password
            def driver = driverMap.driver

            def sql = Sql.newInstance("${instance}", "${user}", "${password}", "${driver}")
	    String labOrmQuery = labOrmSqlQuery()
            String Query = """  ${labOrmQuery} 
                                ${QueryDate} 
                            """

            log.info( "running Query " + Query)
	    log.info("labOrmQuery: ${labOrmQuery}")
            if (sql) {
                log.error("[Message labOrmQuery: ${serviceName}]: Database Connected for Query [${Query}]")
            }

		Map orderIdMap = [:]
		List specimenArr = [];
		List specimen_ModifiedDate_List = new ArrayList();
            String specimen_ModifiedDate_String
            sql.eachRow(Query)
                    { row ->

					if(row.REGISTRATIONNO.find(/(?i)^\s*(GG)\.*/)){
						log.error("ERROR: Patient record found with  patientId starting with GG: "+row.REGISTRATIONNO+".Skipping this record !!\n");
						return
					}

						boolean isReconSuccess = false
                        specimen_ModifiedDate_String = row.ORDERID + "_" + row.MODIFIED_DATE
						if(isReconJob == true){
                            if(specimen_ModifiedDate_List.contains(specimen_ModifiedDate_String)){
				log.info("[Message: specimen_ModifiedDate_String: ${specimen_ModifiedDate_String} matched previous row iteration, hence skipping for next row")
                            }
                            else{
                                isReconSuccess = labOrmReconInsert(row)
				log.info("isReconSuccess: ${isReconSuccess}")
                                if(!isReconSuccess){
                                    return
                                }
                            }
						}
						def groupOrderId = ""
						String orderID = ""
						def sectionCode = ""
						def testID = ""
						def testName = ""
						def orderCategory = ""
						def orderTypeCode = ""
						def ordLongDesc = ""
						def referralDoctorId = ""
						def referralDoctorPrefixName = ""
						def referralDoctorFirstName = ""
						def referralDoctorMiddleName = ""
						def referralDoctorLastName = ""
						def orderStatus =""
						def patientId = ""
						def inPatientOrOut = ""
						def patientPhoneNo = ""
						def patientEmailAddress =""
						def patientNamePrefix = ""
						def firstName = ""
						def lastName = ""
						def middleName = ""
						def patientGender = ""
						def dateofBirth = ""
						def IACODE = ""
						def orderingLocation = ""
						def encounterId = ""
						def orderType = ""
						def orderDatetime = ""
						def modifiedDate =""
						def encounterStartDate = ""
						def encounterEndDate = ""
						def reflexTest = ""
						def groupTest = ""
						def batteryCount = ""
						def nablFlag = ""

						if (row.GROUPORDERID)
							groupOrderId = row.GROUPORDERID
						if (row.ORDERID)
							orderID = "${row.ORDERID}"
						if (row.SECTION_CODE)
							sectionCode = row.SECTION_CODE
						if (row.TESTID)
							testID = row.TESTID
						if (row.TESTNAME)
							testName = row.TESTNAME
						if (row.ORDERCATEGORY)
							orderCategory = row.ORDERCATEGORY
						if (row.ORDERTYPECODE)
							orderTypeCode = row.ORDERTYPECODE
						if (row.ORDLONGDESC)
							ordLongDesc = row.ORDLONGDESC
						if (row.REFDOCTORID)
							referralDoctorId = row.REFDOCTORID
						if (row.REFERRALDOCTORNAME)
						{
							(referralDoctorPrefixName,referralDoctorFirstName,referralDoctorMiddleName,referralDoctorLastName) = getNameFromString(row.REFERRALDOCTORNAME)
						}
						if (row.ORDER_STATUS)
							orderStatus = row.ORDER_STATUS
						if (row.REGISTRATIONNO)
							patientId = row.REGISTRATIONNO
						if (row.INPATIENTOROUT)
							inPatientOrOut = row.INPATIENTOROUT
						if (row.PATIENTPHONENO)
							patientPhoneNo = row.PATIENTPHONENO
						if (row.PATIENTEMAILADDRESS)
							patientEmailAddress = row.PATIENTEMAILADDRESS
						if (row.PATIENTNAMEPREFIX)
							patientNamePrefix = row.PATIENTNAMEPREFIX
						if (row.FIRSTNAME)
							firstName = row.FIRSTNAME
						if (row.LASTNAME)
							lastName = row.LASTNAME
						if(row.MIDDLENAME)
							middleName = row.MIDDLENAME
						if (row.PATIENTGENDER)
							patientGender = row.PATIENTGENDER
						if (row.DATEOFBIRTH)
							dateofBirth = row.DATEOFBIRTH
						if (row.IACODE)
							IACODE = row.IACODE
						if (row.ORDERINGLOCATION)
							orderingLocation = row.ORDERINGLOCATION
						if (row.ENCOUNTERID)
							encounterId = "${row.ENCOUNTERID}"
						if (row.ORDERTYPE)
							orderType = row.ORDERTYPE
						if (row.ORDERDATETIME)
							orderDatetime = row.ORDERDATETIME
						if(row.MODIFIED_DATE)
							modifiedDate = row.MODIFIED_DATE
						if(row.VISIT_ADM_DATE_TIME)
							encounterStartDate = row.VISIT_ADM_DATE_TIME
						if(row.DISCHARGE_DATE_TIME)
							encounterEndDate = row.DISCHARGE_DATE_TIME
						if(row.REFLEX_TEST_YN)
							reflexTest = row.REFLEX_TEST_YN
						if(row.BATTERY_COUNT)
							batteryCount = row.BATTERY_COUNT
						if(row.NABLFLAG)
							nablFlag = row.NABLFLAG
						if(row.GROUP_TEST_YN)
							groupTest = row.GROUP_TEST_YN

						Map docMap = [:]
						docMap.put("groupOrderId", groupOrderId)
						docMap.put("orderID", orderID)
						docMap.put("sectionCode", sectionCode)
						docMap.put("testID", testID)
						docMap.put("testName", testName)
						docMap.put("orderCategory", orderCategory)
						docMap.put("orderTypeCode", orderTypeCode)
						docMap.put("ordLongDesc", ordLongDesc)
						docMap.put("refDoctorId", referralDoctorId)
						docMap.put("referralDoctorFirstName", referralDoctorFirstName)
						docMap.put("referralDoctorLastName", referralDoctorLastName)
						docMap.put("referralDoctorMiddleName", referralDoctorMiddleName)
						docMap.put("referralDoctorPrefixName", referralDoctorPrefixName)
						docMap.put("orderStatus", orderStatus)
						docMap.put("patientId", patientId)
						docMap.put("inPatientOrOut", inPatientOrOut)
						docMap.put("patientPhoneNo", patientPhoneNo)
						docMap.put("patientEmailAddress",patientEmailAddress)
						docMap.put("patientNamePrefix", patientNamePrefix)
						docMap.put("firstName", firstName)
						docMap.put("middleName", middleName)
						docMap.put("lastName", lastName)
						docMap.put("patientGender", patientGender)
						docMap.put("dateofBirth", dateofBirth)
						docMap.put("IACODE", IACODE)
						docMap.put("orderingLocation", orderingLocation)
						docMap.put("encounterId", encounterId)
						docMap.put("orderType", orderType)
						docMap.put("orderDatetime", orderDatetime)
						docMap.put("modifiedDate",modifiedDate)
						docMap.put("encounterStartDate", encounterStartDate)
						docMap.put("encounterEndDate", encounterEndDate)
						docMap.put("reflexTest", reflexTest)
						docMap.put("groupTest", groupTest)
						docMap.put("batteryCount", batteryCount)
						docMap.put("status", "PENDING")
						docMap.put("retries", 0)
                        docMap.put("nablFlag", nablFlag)
						/*
						if(reflexTest == "Y"){
							logger.error("Patient record found with reflexTest: ${reflexTest} and patientId : "+row.REGISTRATIONNO+".Skipping this record !!\n");
							return;
						}
						*/
                    if(!specimen_ModifiedDate_List.contains(specimen_ModifiedDate_String)){
                        specimen_ModifiedDate_List.add(specimen_ModifiedDate_String)
                    }
					if(!specimenArr.contains(orderID))
                        		specimenArr.add(orderID);
					if(orderIdMap.containsKey(orderID)){
						orderIdMap.get(orderID).add(docMap)
					}else{
						orderIdMap.put(orderID,[docMap]);
					}

                }
		//List uniqSpecimen = distinctSpecimen(QueryDate,serviceName,driverMap);
            	//log.error("UNIQUE SPECIMEN ============="+uniqSpecimen);
            	Map batteryTestCount = batteryCounter(serviceName,driverMap,specimenArr,microbiologyTestHash);
            	//Map batteryTestCount = batteryCounterOld(serviceName,driverMap,specimenArr,microbiologyTestHash);
            	log.error("BATTERY COUNT ============="+batteryTestCount);
		//log.error("OrderMap============"+orderIdMap);
		Map ormHashMap =  [:];
		orderIdMap.each { key,val ->

		Map orderMap = [:]

		val.each { record ->

		if(!ormHashMap.containsKey(record.orderID)){
			orderMap.put("groupOrderId", record.groupOrderId)
			orderMap.put("orderID", record.orderID)
			orderMap.put("sectionCode", record.sectionCode)
			orderMap.put("orderCategory", record.orderCategory)
			orderMap.put("orderTypeCode", record.orderTypeCode)
			orderMap.put("ordLongDesc", record.ordLongDesc)
			orderMap.put("refDoctorId", record.refDoctorId)
			orderMap.put("referralDoctorFirstName", record.referralDoctorFirstName)
			orderMap.put("referralDoctorLastName", record.referralDoctorLastName)
			orderMap.put("referralDoctorMiddleName", record.referralDoctorMiddleName)
			orderMap.put("referralDoctorPrefixName", record.referralDoctorPrefixName)
			orderMap.put("orderStatus", record.orderStatus)
			orderMap.put("patientId", record.patientId)
			orderMap.put("inPatientOrOut", record.inPatientOrOut)
			orderMap.put("patientPhoneNo", record.patientPhoneNo)
			orderMap.put("patientEmailAddress",record.patientEmailAddress)
			orderMap.put("patientNamePrefix", record.patientNamePrefix)
			orderMap.put("firstName", record.firstName)
			orderMap.put("middleName", record.middleName)
			orderMap.put("lastName", record.lastName)
			orderMap.put("patientGender", record.patientGender)
			orderMap.put("dateofBirth", record.dateofBirth)
			orderMap.put("IACODE", record.IACODE)
			orderMap.put("orderingLocation", record.orderingLocation)
			orderMap.put("encounterId", record.encounterId)
			orderMap.put("orderType", record.orderType)
			orderMap.put("orderDatetime", record.orderDatetime)
			orderMap.put("modifiedDate",record.modifiedDate)
			orderMap.put("encounterStartDate", record.encounterStartDate)
			orderMap.put("encounterEndDate", record.encounterEndDate)
			orderMap.put("reflexTest", record.reflexTest)
			orderMap.put("groupTest", record.groupTest)
			orderMap.put("batteryCount", record.batteryCount)
			//orderMap.put("nablFlag", record.nablFlag)
			orderMap.put("status", "PENDING")
			orderMap.put("retries", 0)
		}
                log.info("============== nablFlag======"+record.nablFlag)
                log.info("============== record======"+record)
		String testID = record.testID;
		String testName = record.testName;
		Map obxHash = ["testID":record.testID,"testName":record.testName];
		List testListMap = []
		testListMap.push(obxHash)
		List batteryTestToBeExcludedInMicrobiology = ["3DAYS","OVERNIGHT","LMICGSF166","LMICACF036","LMICAFB486"]
		if(record.groupTest == "N"){
			if(microbiologyTestHash.containsKey(record.testID) && !(batteryTestToBeExcludedInMicrobiology.contains(record?.testID?.toUpperCase()))){
				testID = record.orderID+"_"+record.testID+"_BAT";
				 log.error("In SENSITIVITY ================="+record.testID)
			} else{
				log.error("BATTERY COUNT =================="+batteryTestCount[record.orderID]);
				if(batteryTestCount.containsKey(record.orderID)) {
					if(record?.nablFlag?.equals("YES") && batteryTestCount[record.orderID].containsKey('yesNabl') && batteryTestCount[record.orderID]['yesNabl'] > 1){
						testID = record.orderID+"_NABL_BAT";
						testName = "Multiple Test"
					}
					else if(record?.nablFlag?.equals("NO") && batteryTestCount[record.orderID].containsKey('noNabl') && batteryTestCount[record.orderID]['noNabl'] > 1){
						testID = record.orderID+"_BAT";
						testName = "Multiple Test"
					}
				}
			}

		}
		if(batteryTestCount.containsKey(record.orderID)){
			orderMap["batteryCount"] = batteryTestCount[record.orderID]['yesNabl']+batteryTestCount[record.orderID]['noNabl']
		}

		if(ormHashMap.containsKey(record.orderID)){
		if(ormHashMap[record.orderID].containsKey("obrMap")){
				if(ormHashMap[record.orderID]['obrMap'].containsKey(testID)){
					ormHashMap[record.orderID]['obrMap'][testID]['nablFlag'] = record?.nablFlag
					if(ormHashMap[record.orderID]['obrMap'][testID].containsKey('obxMap')){
						 ormHashMap[record.orderID]['obrMap'][testID]['obxMap'].push(obxHash);
						if(ormHashMap[record.orderID]['obrMap'][testID]['obxMap'].size() > 1){
							testName = "Multiple Test";
							ormHashMap[record.orderID]['obrMap'][testID]["testName"] = testName;
						}
					}

				}else{
					ormHashMap[record.orderID]['obrMap'][testID] = ["testID":testID,"testName":testName, "nablFlag":  record?.nablFlag];

					ormHashMap[record.orderID]['obrMap'][testID].put("obxMap",testListMap)
				}
				log.error("In if2:"+ ormHashMap[record.orderID])


		}
		}else{
				Map testNameMap = [:]
				testNameMap[testID] = ["testID":testID,"testName":testName]
				testNameMap[testID].put("obxMap",testListMap)
				testNameMap[testID].put("nablFlag",record?.nablFlag)
				log.error("In else2:"+testNameMap)
				orderMap.put("obrMap", testNameMap)
				ormHashMap[record.orderID] = orderMap;
		}
	}
}

log.error("FINAL HASH:================== "+ormHashMap);
				ormHashMap.each { key,val ->

					isSuccess = false
					isSuccess = save(val, collectionName)
					if (isSuccess) {
						counter++
						log.info "[Message: ${serviceName}]: Inserted document entry  in ${collectionName} with status [PENDING]"
					}
				}

            sql.close()
            return counter
        }
        catch (Exception ex) {
            log.error("Error: Exception occurred to process ${collectionName} ${serviceName} custom job ", ex)
            throw ex
        }
    }

public boolean labOrmReconInsert(def rowMap){
	//Fetching the data from labOrmReportDump
	boolean isSuccess = false
	String patientId = rowMap.REGISTRATIONNO
	String specimenNumber =  rowMap.ORDERID
	String modifiedDate = rowMap.MODIFIED_DATE
	log.info("Inside labOrmReconInsert function!")
	if((patientId && patientId != '') && (specimenNumber && specimenNumber != '') && (modifiedDate && modifiedDate != '')) {
		String searchMap = ""
		String collectionName = "labOrmReportDump"
		String sortMap = ""
		int pageSize = 1
		int offset = 0
		Map searchResult = [:]
		searchMap = '{"$and":[{"patientId" : "'+patientId+'"},{"orderID" : "'+specimenNumber+'"},{"status":{"$ne":"FAILED"}}]}'
		sortMap = '{_id : -1}'
		searchResult = search(searchMap, collectionName, sortMap, pageSize, offset)
		if (searchResult.matchCount == 0) {
			log.info("[Message: ${serviceName}]: No instance of patientID [${patientId}] with specimenNumber [${specimenNumber}] exists in [${collectionName}]. Going to insert record")
			isSuccess = true
		} else {
			searchResult.objects.each {
				long _id = it._id
				String oldmodifiedDate = it.modifiedDate
				if (oldmodifiedDate && oldmodifiedDate != '') {
					long oldModified = oldmodifiedDate.toLong()
					long newModified = modifiedDate.toLong()
					if (newModified > oldModified) {
						log.info("[Message: ${serviceName}]: Going to insert instance with patientID [${patientId}] with specimenNumber [${specimenNumber}] as new ModifiedDate ${modifiedDate} is greater than old ModifiedDate [${oldmodifiedDate}]")
						isSuccess = true
					} else {
						log.info("[Message: ${serviceName}]: Skipping to insert instance with patientID [${patientId}] with specimenNumber [${specimenNumber}] as new ModifiedDate ${modifiedDate} is not greater than old ModifiedDate [${oldmodifiedDate}] ")
						isSuccess = false
					}
				}
			}
		}
	}
	else
	{
		log.info("[Message: ${serviceName}]: Skipping to insert instance because either  patientID [${patientId}] or specimenNumber [${specimenNumber}] or ModifiedDate ${modifiedDate} is empty")
		isSuccess = false
	}
	return isSuccess
}


public def checkSensitivityTestCode(String serviceName,Map driverMap) {

	List testCodeArr = [];
	int counter = 0
    	Boolean isSuccess = false
	Map testMap = [:]
	String searchMap = ""
	String sortMap = ""
	String searchStatus = "PENDING"
	String collectionName = "organismTestCode"
	int pageSize = 200
	int offset = 0
	Map searchResult = [:]
	sortMap = '{_id : 1}'
	searchResult = search (searchMap, collectionName, sortMap,pageSize, offset)
	if (searchResult.matchCount == 0) {
		log.info ("[Message: ${serviceName}]: Nothing in ${collectionName}. No data left for processing." )
	}
	else
	{
		log.info ("[Message checkSensitivityTestCode: ${serviceName}]: Number of records fetched : ${searchResult.matchCount}")
		searchResult.objects.each {
			testMap[it.testCode] = 1
		}
	}

	def instance = driverMap.instance
	def user = driverMap.user
	def password = driverMap.password
	def driver = driverMap.driver
	def sql = Sql.newInstance("${instance}", "${user}", "${password}", "${driver}")

	String Query = """select distinct test_code from rl_result_organism_dtl where test_code = group_test_code""";
	println "running Query " + Query
	if (sql) {
		log.error("[Message checkSensitivityTestCode: ${serviceName}]: Database Connected for Query [${Query}]")
	}
	def HISTestCodeMap = [:]
	sql.eachRow(Query)
			{ row ->
				HISTestCodeMap[row.TEST_CODE]=1
			}
	testMap.each{
		if(!HISTestCodeMap.containsKey(it.key)){
			log.info("[Message checkSensitivityTestCode: ${serviceName}]: testCode status updating to false")
			def upSearch = '{"testCode":"'+it.key+'"}'
			def updateMap = '{"$set":{"status": false}}'
			def updateStatus = update(upSearch,updateMap,collectionName,false,false)
			if(updateStatus){
				log.info("[Message checkSensitivityTestCode: ${serviceName}]: testCode status updated to false")
			}
			else{
				log.info("[Message checkSensitivityTestCode: ${serviceName}]: error to update testCode status")
			}
		}
	}
	sql.eachRow(Query)
	{ row ->
		if(testMap.containsKey(row.TEST_CODE)){
			log.info ("[Message: ${serviceName}]: Collection ${collectionName} already contains testcode: [row.TEST_CODE]")
		}else{
			Map codeMap = [:]
			codeMap.put("testCode", row.TEST_CODE)
			codeMap.put("insertISODate", new Date())

			log.error ("[Message: ${serviceName}]: Collection ${collectionName} doesnot contain testcode: [row.TEST_CODE]. Going to insert the record.")
			isSuccess = false
            		isSuccess = save(codeMap, collectionName)
			if (isSuccess) {
				counter++
				testCodeArr.add(row.TEST_CODE);
				log.info "[Message: ${serviceName}]: Inserted document entry  in collection: ${collectionName} "
			}

		}

	}

	sql.close()
	return [testCodeArr,counter];
}

public def specimenBatteryCount(String serviceName,Map driverMap,List specarray,Map microbiologyHash,int counter){
 log.error("Function called with counter:"+counter);
    String specimenStr = '';
    int incrementer = 1;
    for(i in counter .. specarray.size()-1) {
        //log.error(specarray.get(i))
        if(incrementer > 2){
            break;
        }
        if(specimenStr != ''){
			specimenStr += ",'"+specarray.get(i)+"'";
        }else{
			specimenStr = "'"+specarray.get(i)+"'";
        }
        incrementer++;
        counter++;
    }

	def instance = driverMap.instance
	def user = driverMap.user
	def password = driverMap.password
	def driver = driverMap.driver

	def sql = Sql.newInstance("${instance}", "${user}", "${password}", "${driver}")

	Map countMap = [:]

		//String qry = "select specimen_no, test_code  from rl_request_detail where group_test_yn = 'N' and specimen_no in (" +specimenNoStr + ")";
		String qry = "select h.specimen_no, rl.test_code, nvl(nb.Remarks2,'NO') nabl_flag from rl_request_header h,rl_test_code rl,or_order_line ol,or_order o, RL_REFER_IN_TEST_CODE nb where o.order_id = ol.order_id and o.order_id = h.order_id and o.patient_id = h.patient_id and rl.test_code = ol.order_catalog_code and rl.group_test_yn = 'N' and ol.order_catalog_code = nb.test_code and o.ordering_facility_id= nb.Operating_Facility_ID and h.specimen_no in (" + specimenStr + ")";

		log.error ("Battery Count running Query " + qry)
		if (sql) {
			log.error("[Message specimenBatteryCount: ${serviceName}]: Database Connected for Query [${qry}]")
		}

		sql.eachRow(qry)
		{ row ->
				if(!microbiologyHash[row.TEST_CODE]) {
						String specimen = row.SPECIMEN_NO;
					if(countMap.containsKey(specimen)) {
						if(nabl == 'YES'){
							countMap[specimen]['yesNabl'] += 1
						}
						else{
							countMap[specimen]['noNabl'] += 1
						}
					}
					else{
						if(nabl == 'YES'){
							countMap[specimen] = ['yesNabl': 1]
							countMap[specimen].put('noNabl' , 0)
						}
						else{
							countMap[specimen] = ['yesNabl': 0]
							countMap[specimen].put('noNabl' , 1)
						}
					}
				}
		 }

    sql.close()
    return [counter,specimenStr,countMap]
}

public Map batteryCounter(String serviceName,Map driverMap,List specimenArr,Map microbiologyHash) {
	def specCounter= 0;
	//int loopcount = 0;
	int specimenArrSize = specimenArr.size();
	Map finalbatteryMap = [:];
	if(specimenArrSize > 0){
		while(specimenArrSize > specCounter){
			int count;
			String specstr;
			Map batteryMap = [:];
			(count,specstr,batteryMap)	= specimenBatteryCount (serviceName,driverMap,specimenArr,microbiologyHash,specCounter);
			finalbatteryMap.putAll(batteryMap)
			specCounter= count;
			log.error("String returned: "+specstr)
			log.error("Counter returned: "+count)
			log.error("Specimen Counter: "+specCounter+"\n\n")
			//loopcount++;

			String specimenNoStr = specstr;

	   }

	}else{
         log.error("Specimen Array is empty!!")
    }

        return finalbatteryMap;
}

public Map batteryCounterOld(String serviceName,Map driverMap,List specimenArray,Map microbiologyHash) {
		def instance = driverMap.instance
		def user = driverMap.user
		def password = driverMap.password
		def driver = driverMap.driver

		def sql = Sql.newInstance("${instance}", "${user}", "${password}", "${driver}")

        Map countMap = [:]
        if(specimenArray.size() > 0) {
            String specimenNoStr = specimenArray.join(',');
            //String qry = "select specimen_no, test_code  from rl_request_detail where group_test_yn = 'N' and specimen_no in (" +specimenNoStr + ")";
	    String qry = "select h.specimen_no, rl.test_code from rl_request_header h,rl_test_code rl,or_order_line ol,or_order o where o.order_id = ol.order_id and o.order_id = h.order_id and o.patient_id = h.patient_id and rl.test_code = ol.order_catalog_code and rl.group_test_yn = 'N' and h.specimen_no in (" + specimenNoStr + ")";
            println "Battery Count running Query " + qry
            if (sql) {
                log.error("[Message batteryCounterOld: ${serviceName}]: Database Connected for Query [${qry}]")
            }

            sql.eachRow(qry)
				{ row ->
					if(!microbiologyHash[row.TEST_CODE]) {
						String specimen = row.SPECIMEN_NO;
						if(countMap.containsKey(specimen)) {
							countMap[specimen] += 1
						}else {
							countMap[specimen] = 1
						}
					}

				}
        }
	sql.close()
        return countMap;
    }

public Map organismTestCodeHash(String serviceName) {

			Map testMap = [:]
			//Fetching the data from organismTestcode
			String searchMap = '{"status":true}'
			String sortMap = ""
			String searchStatus = "PENDING"
			String collectionName = "organismTestCode"
			int pageSize = 200
			int offset = 0
			Map searchResult = [:]

			//searchMap = '{"status" : "' + searchStatus + '"}'
			sortMap = '{_id : 1}'
			searchResult = search (searchMap, collectionName, sortMap,pageSize, offset)
			if (searchResult.matchCount == 0) {
				log.info ("[Message organismTestCodeHash: ${serviceName}]: Nothing in ${searchStatus} in ${collectionName}. No data left for processing." )
			}
			else
			{
				log.info ("[Message organismTestCodeHash: ${serviceName}]: Number of records fetched : ${searchResult.matchCount}")
				searchResult.objects.each {
					testMap[it.testCode] = 1
				}
			}

			return testMap;
	}

public def distinctSpecimen(String QueryDate,String serviceName,Map driverMap) {

	    def instance = driverMap.instance
            def user = driverMap.user
            def password = driverMap.password
            def driver = driverMap.driver

            def sql = Sql.newInstance("${instance}", "${user}", "${password}", "${driver}")
			//String q = """select distinct h.specimen_no SPECIMEN from or_order o,rl_request_header h  where o.order_id = h.order_id and o.patient_id = h.patient_id and o.order_status = 'RG' """;
		String q = """select distinct h.specimen_no SPECIMEN from or_order o,rl_request_header h  where o.order_id = h.order_id and o.patient_id = h.patient_id and o.order_category = 'LB' and o.order_status in ('OS','RG','IM') and o.modified_date between to_date('2018-04-17 00:00:00', 'YYYY-MM-DD HH24:MI:SS') and to_date('2018-04-17 23:59:59', 'YYYY-MM-DD HH24:MI:SS')""";
			//and o.modified_date between to_date('${startDateStr}', 'YYYY-MM-DD HH24:MI:SS') and to_date('${endDateStr}', 'YYYY-MM-DD HH24:MI:SS')
            String Query = """  ${q} 
                            """
            println "running Query " + Query
            if (sql) {
                log.error("[Message distinctSpecimen: ${serviceName}]: Database Connected for Query [${Query}]")
            }

			List specimenArr = [];
            sql.eachRow(Query)
                    { row ->
						if(!specimenArr.contains(row.SPECIMEN))
                        specimenArr.add(row.SPECIMEN);
					}
			sql.close()
			return specimenArr;
	}

public String labOrmSqlQuery(){
        String Query = """select
			oc.order_category orderCategory,
			ordtyp.order_type_code orderTypeCode,
			ordtyp.long_desc ordlongdesc,
			o.order_id grouporderid,
			h.specimen_no orderid,
			rl_section_code.long_name section_code,
			ol.order_catalog_code testid,
			--ol.catalog_desc testname,
			nvl(testCode.billing_name,testCode.test_name) testname,
			prenctr.attend_practitioner_id refdoctorid,
			refdoc.practitioner_name referraldoctorname,
			o.order_status,
			o.patient_id registrationno,
			o.patient_class inpatientorout,
			nvl(p.contact1_no, p.contact2_no) patientphoneno,
			p.email_id patientemailaddress,
			p.name_prefix patientnameprefix,
			p.first_name firstname,
			p.family_name lastname,
			p.second_name middlename,
			p.sex patientgender,
			to_char(p.date_of_birth, 'yyyymmdd') dateofbirth,
			o.ordering_facility_id iacode,
			nvl(clinic.long_desc, ip_clinic.long_desc) orderinglocation,
			o.encounter_id encounterid,
			o.order_category ordertype,
			to_char(o.ord_date_time, 'YYYYMMDDHH24MISS') orderdatetime,
			to_char(o.modified_date, 'YYYYMMDDHH24MISS') modified_date,
			to_char(prenctr.visit_adm_date_time, 'YYYYMMDDHH24MISS') visit_adm_date_time,
			to_char(prenctr.discharge_date_time, 'YYYYMMDDHH24MISS') discharge_date_time,
			testCode.REFLEX_TEST_YN,
			testCode.group_test_yn,
			'0' BATTERY_COUNT,
			nvl((SELECT nb.Remarks2 FROM RL_REFER_IN_TEST_CODE nb WHERE ol.order_catalog_code = nb.test_code and o.ordering_facility_id= nb.Operating_Facility_ID), 'NO') nablFlag
			from 
			or_order o,
			or_order_catalog oc,
			or_order_line ol,
			or_order_type ordTyp,
			MPHRX_MPPT_VW p,
			rl_request_header h,
			am_practitioner refdoc,
			op_clinic clinic,
			ip_nursing_unit ip_clinic,
			--RL_TEST_CODE testCode,
			mphrx_test_dtls testCode,
			rl_section_code,
			PR_Encounter prenctr
			where 
			o.order_id = ol.order_id 
			and o.order_category = oc.order_category
			and ol.order_catalog_code = oc.order_catalog_code
			and ol.order_type_code = oc.order_type_code
			and o.order_type_code = ordTyp.order_type_code
			and ip_clinic.facility_id (+) = o.ordering_facility_id
			and ip_clinic.nursing_unit_code(+) = o.source_code
			and clinic.facility_id(+) = o.ordering_facility_id
			and clinic.clinic_code(+) = o.source_code
			and refdoc.practitioner_id(+) = prenctr.ATTEND_PRACTITIONER_ID
			and o.encounter_id=prenctr.encounter_id
			and o.ordering_facility_id=prenctr.facility_id
		        and o.order_id = h.order_id
			and o.patient_id = h.patient_id
			and p.patient_id = o.patient_id
			and testCode.section_code = rl_section_code.section_code
			and testCode.test_code = oc.order_catalog_code
			and oc.order_category = 'LB'
		      	and o.order_status in ('OS','RG','IM','HD','CN','DC','RJ')
			--and o.order_status = 'RG'
			and testCode.REFLEX_TEST_YN = 'N'
			and testCode.test_code <> 'LHISRE001'
			--and o.modified_date between to_date('2017-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') and to_date('2017-01-01 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
                        --and h.specimen_no in ('4021000029','1021000102')
			--and h.specimen_no in ('7016008671','7016009774','5017009951','7016014804')
			--and h.specimen_no in ('4018007270')
            --AND ROWNUM < 10
            """
        return Query

    }

public def opdQuery(String QueryDate,String serviceNameOrg,Map driverMap,boolean isReconJob = false,String FileStorageLocation)
    {
        int counter = 0
        Boolean isSuccess = false
        String collectionName ="opdDocumentDump"
        serviceName = serviceNameOrg;

        try{
            def instance = driverMap.instance
            def user = driverMap.user
            def password = driverMap.password
            def driver = driverMap.driver

            def sql = Sql.newInstance("${instance}", "${user}", "${password}", "${driver}")
            String opdNotesQuery = opdNotesSqlQuery()
            String opdNotesUnionQuery = opdNotesUnionSqlQuery()
            String Query = """  ${opdNotesQuery} 
                                    ${QueryDate} 
                 ${opdNotesUnionQuery}
                ${QueryDate}
                                """
//  ${QueryDate}
                println "running UNION_OPDNOTES Query "+Query
                if(sql)
                {
                    log.error ("[Message opdQuery: ${serviceName}]: Database Connected for Query [${Query}]")
                }
                sql.eachRow(Query)
                        {
                            row ->
                                Map docMap = [:]
                                String patId = "";
                                if(row.REGISTRATIONNO)
                                    patId = row.REGISTRATIONNO

                                if(patId.find(/(?i)^\s*(GG)\.*/)){
                                    println ("Patient record found with  patId starting with GG: "+patId+".Skipping this record !!\n\n");
                                    return
                                }
                                boolean isReconSuccess = false
                                if(isReconJob == true){
                                    isReconSuccess = opdReconInsert(row,FileStorageLocation)
                                    if(!isReconSuccess){
                                        return
                                    }
                                }

                                // variables for store data in mongo for MDM type messages
                                def patientId = ""
                                def patientTitle = ""
                                def patientFirstName =""
                                def patientMiddleName =""
                                def patientLastName =""
                                def msgPatientDob =""
                                def patientSex = ""
                                def patientHomePh =""
                                def patientEmail =""
                                def physicianId =""
                                def physicianFirstName = ""
                                def physicianMiddleName = ""
                                def physicianLastName = ""
                                def physicianTitle = ""
                                def patientClass = ""
                                def patientVisitNumber = ""
                                def encounterStartDate = ""
                                def encounterEndDate = ""
                                def msgOriginationDate = ""
                                def documentNumber = ""
                                def facility_id = ""
                                def location = ""
                                def documentFileName = "" //document description in Minerva GUI
                                def fileLocation = ""
                                def documentType = "OPD Notes" //document Type in Minerva GUI
                                def documentContentPresentation = ""  //this is mime type
                                def authorisedDocName = ""
                                def authorisedDate = ""
                                def noteGroup = ""
                                def xmlContent = ""
                                def dcs_xmlContent = ""
                                def eventCode = ""
								def documentStatus = ""

                                if(row.REGISTRATIONNO)
                                    patientId = row.REGISTRATIONNO
                                if(row.PATIENTPHONENO)
                                    patientHomePh = row.PATIENTPHONENO
                                if(row.PATIENTEMAILADDRESS)
                                    patientEmail = row.PATIENTEMAILADDRESS
                                if(row.PATIENTTITLE)
                                    patientTitle = row.PATIENTTITLE
                                if(row.FIRSTNAME)
                                    patientFirstName = row.FIRSTNAME
                                if(row.LASTNAME)
                                    patientLastName = row.LASTNAME
                                if(row.MIDDLENAME)
                                    patientMiddleName = row.MIDDLENAME
                                if(row.PATIENTGENDER)
                                    patientSex = row.PATIENTGENDER
                                if(row.DATEOFBIRTH)
                                    msgPatientDob = row.DATEOFBIRTH
                                if(row.ENCOUNTERID)
                                    patientVisitNumber= "${row.ENCOUNTERID}"
                                if(row.MSGORIGINATIONDATE)
                                    msgOriginationDate = row.MSGORIGINATIONDATE
                                if(row.REFDOCTORID)
                                    physicianId = row.REFDOCTORID
                                if(row.OBJTYPE)
                                    documentContentPresentation = row.OBJTYPE
                                if(row.INPATIENTOROUT)
                                    patientClass =row.INPATIENTOROUT
                                if(row.FACILITY_ID)
                                    facility_id = row.FACILITY_ID
                                if(row.LOCATION)
                                    location = row.LOCATION
                                if(row.VISITDATE)
                                    encounterStartDate = row.VISITDATE
                                if(row.DISCHARGEDATE)
                                    encounterEndDate = row.DISCHARGEDATE

                                if(row.AUTHORISEDDOCNAME)
                                    authorisedDocName = row.AUTHORISEDDOCNAME
                                if(row.AUTHORISEDDATE)
                                    authorisedDate = row.AUTHORISEDDATE
                                if(row.XMLCONTENT)
                                    xmlContent = row.XMLCONTENT
								if(row.DCS_XMLCONTENT)
                                    dcs_xmlContent = row.DCS_XMLCONTENT
								if(row.EVENT_CODE)
                                    eventCode = row.EVENT_CODE
								if(row.documentStatus){
									documentStatus = row.documentStatus
								}else {
									documentStatus = ""
								}

                                if(row.NOTEGROUP)
                                    noteGroup = row.NOTEGROUP
                                //variables for fetch data from above query
                                def EXT_IMAGE_APPL_ID = row.EXT_IMAGE_APPL_ID
                                def REFERRALDOCTORNAME = row.REFERRALDOCTORNAME
                                Boolean insertFlag =false


                                if(EXT_IMAGE_APPL_ID && EXT_IMAGE_APPL_ID != null && EXT_IMAGE_APPL_ID !="")
                                {
                                    (documentNumber,documentFileName,fileLocation) = getFileFromBlob(row,FileStorageLocation)
                                }
                                else if(xmlContent){
                                    (documentNumber,documentFileName,fileLocation) = getPdfFromXml(row)
                                    documentContentPresentation = "application/pdf"
                                }
								else if(dcs_xmlContent){
                                    (documentNumber,documentFileName,fileLocation) = getPdfFromDcsXml(row)
                                    documentContentPresentation = "application/pdf"
                                }
                                else
                                {
                                    (documentNumber,documentFileName,fileLocation) = getPdfFromHtml(row,FileStorageLocation)
                                    documentContentPresentation = "application/pdf"
                                }
                                if(documentNumber && documentFileName)
                                    insertFlag = true
                                else
                                    return
                                if(row.NOTE_TYPE_DESC)
                                {
                                    documentFileName = row.NOTE_TYPE_DESC
                                    if(noteGroup == "D001" || noteGroup == "D002"){
                                        documentType = "Clinical Diagnostic"
                                    }
                                    else if(patientClass == "IP" || patientClass == "DC"){
                                        documentType = "Discharge Note"
                                    }


                                }
                                else
                                {

                                    if(noteGroup == "D001" || noteGroup == "D002"){
                                        documentFileName = "Clinical Diagnostic"
                                        documentType = "Clinical Diagnostic"
                                    }
                                    else if(patientClass == "IP" || patientClass == "DC"){
                                        documentFileName = "Discharge Note"
                                        documentType = "Discharge Note"
                                    }else{
                                        documentFileName = "OPD Notes"
                                    }

                                }


                                if(REFERRALDOCTORNAME && REFERRALDOCTORNAME != null)
                                {
                                    (physicianTitle,physicianFirstName,physicianMiddleName,physicianLastName) = getNameFromString(REFERRALDOCTORNAME)
                                }

                                docMap.put("patientId", patientId)
                                docMap.put("status", "PENDING")
                                docMap.put("retries", 0)
                                docMap.put("patientFirstName", patientFirstName)
                                docMap.put("patientTitle", patientTitle)
                                docMap.put("patientMiddleName", patientMiddleName)
                                docMap.put("patientLastName", patientLastName)
                                docMap.put("msgPatientDob", msgPatientDob)
                                docMap.put("patientSex", patientSex)
                                docMap.put("patientHomePh", patientHomePh)
                                docMap.put("patientEmail", patientEmail)
                                docMap.put("physicianId", physicianId)
                                docMap.put("physicianFirstName", physicianFirstName)
                                docMap.put("physicianMiddleName", physicianMiddleName)
                                docMap.put("physicianLastName", physicianLastName)
                                docMap.put("physicianTitle", physicianTitle)
                                docMap.put("patientClass", patientClass)
                                docMap.put("patientVisitNumber", patientVisitNumber)
                                docMap.put("encounterStartDate", encounterStartDate)
                                docMap.put("encounterEndDate", encounterEndDate)
                                docMap.put("msgOriginationDate", msgOriginationDate)
                                docMap.put("documentNumber", documentNumber)
                                docMap.put("documentFileName", documentFileName)
                                docMap.put("fileLocation", fileLocation)
                                docMap.put("documentType", documentType)
                                docMap.put("facilityId", facility_id)
                                docMap.put("location", location)
                                docMap.put("authorisedDocName", authorisedDocName)
                                docMap.put("authorisedDate", authorisedDate)
                                docMap.put("documentContentPresentation", documentContentPresentation)
                                docMap.put("noteGroupId", noteGroup)
								docMap.put("documentStatus", documentStatus)
								docMap.put("eventCode", eventCode);

                                // docMap.put("Directory", row.Directory)
                                isSuccess = false
                                isSuccess = save (docMap, collectionName)
                                //isSuccess = comFun.save(docMap, "questionerDump")
                                if (isSuccess) {
                                    counter++
                                    log.info "[Message: ${serviceName}]: Inserted document entry documentNumber[${documentNumber}] pkID[${patientId}] in ${collectionName} with status [PENDING]"
                                }else{
                                    Log.error "[Message: ${serviceName}]: ERROR: Failed to insert document entry pkID[${patientId}] in ${collectionName} with status [PENDING]"
                                }
                        }
                sql.close()
                return counter
        }
        catch (Exception ex)
        {
            log.error("Error: Exception occurred to process ${collectionName} ${serviceName} custom job ", ex)
            throw ex
        }
    }

    public boolean opdReconInsert(def rowMap,String FileStorageLocation){
        //Fetching the data from radReportDump
        boolean isSuccess = false
        boolean isDocumentNoRequired = false
        def documentNumber = ""
        String FILE_NAME = rowMap.FILE_NAME
        String ACCESSION_NUM =  rowMap.ACCESSION_NUM
        String ENCOUNTERID = rowMap.ENCOUNTERID
        String patientId = rowMap.REGISTRATIONNO
        def EXT_IMAGE_APPL_ID = rowMap.EXT_IMAGE_APPL_ID
        String modifiedDate = rowMap.AUTHORISEDDATE

        if(EXT_IMAGE_APPL_ID && EXT_IMAGE_APPL_ID != null && EXT_IMAGE_APPL_ID !="")
        {
            isDocumentNoRequired = true
            (documentNumber) = getFileFromBlob(rowMap,FileStorageLocation,isDocumentNoRequired)
        }
        else
        {
            isDocumentNoRequired = true
            (documentNumber) = getPdfFromHtml(rowMap,FileStorageLocation,isDocumentNoRequired)
        }
        if(documentNumber && documentNumber !="" ) {
            String searchMap = ""
            String collectionName = "opdDocumentDump"
            String sortMap = ""
            int pageSize = 1
            int offset = 0
            Map searchResult = [:]
            searchMap = '{"$and":[{"patientId" : "'+patientId+'"},{"documentNumber" : "'+documentNumber+'"},{"status":{"$ne":"FAILED"}}]}'
            sortMap = '{_id : -1}'
            searchResult = search(searchMap, collectionName, sortMap, pageSize, offset)
            if (searchResult.matchCount == 0) {
                log.info("[Message: ${serviceName}]: No instance of patientID [${patientId}] with documentNumber [${documentNumber}] exists in [${collectionName}]. Going to insert record")
                isSuccess = true
            } else {
                searchResult.objects.each {
                    long _id = it._id
                    String oldmodifiedDate = it.authorisedDate
                    if (oldmodifiedDate && oldmodifiedDate != '') {
                        long oldModified = oldmodifiedDate.toLong()
                        long newModified = modifiedDate.toLong()
                        if (newModified > oldModified) {
                            log.info("[Message: ${serviceName}]: Going to insert instance with ENCOUNTERID [${ENCOUNTERID}] with ACCESSION_NUM [${ACCESSION_NUM}] documentNumber [${documentNumber}]  as new ModifiedDate ${modifiedDate} is greater than old ModifiedDate [${oldmodifiedDate}]")
                            isSuccess = true
                        } else {
                            log.info("[Message: ${serviceName}]: Skipping to insert instance with ENCOUNTERID [${ENCOUNTERID}] with ACCESSION_NUM [${ACCESSION_NUM}] documentNumber [${documentNumber}]  as new ModifiedDate ${modifiedDate} is not greater than old ModifiedDate [${oldmodifiedDate}] ")
                            isSuccess = false
                        }
                    }
                }
            }
        }
        else
        {
            log.info("[Message: ${serviceName}]: Skipping to insert instance because either  ENCOUNTERID [${ENCOUNTERID}] or ACCESSION_NUM [${ACCESSION_NUM}] or FILE_NAME ${FILE_NAME} is empty, failed to fetch documentNumber")
            isSuccess = false
        }
        return isSuccess
    }
    public String opdNotesSqlQuery(){
        String Query = """SELECT
  nt.NOTE_GROUP_ID NOTEGROUP,
  p.patient_id RegistrationNo,
  NVL(p.contact1_no,p.contact2_no) PatientPhoneNo,
  c.status documentStatus,
  p.EMAIL_ID PatientEmailAddress,
  p.first_name FirstName,
  p.family_name LastName,
  p.second_name middleName,
  p.name_prefix patientTitle,
  p.sex PatientGender,
  TO_CHAR(p.date_of_birth , 'yyyymmdd') DateofBirth,
  c.patient_class InPatientOrOut,
  c.encounter_id EncounterId,
  TO_CHAR(c.event_date, 'YYYYMMDDHH24MISS') msgOriginationDate,
  TO_CHAR(e.VISIT_ADM_DATE_TIME, 'YYYYMMDDHH24MISS') VISITDATE,
  TO_CHAR(e.DISCHARGE_DATE_TIME, 'YYYYMMDDHH24MISS') dischargeDate,
  c.EXT_IMAGE_APPL_ID,
  co.EVENT_TITLE,
  co.FILE_NAME,
  co.HIST_DATA,
  co.OBJTYPE,
  e.ATTEND_PRACTITIONER_ID RefDoctorId,
  pr.practitioner_name ReferralDoctorName,
  spec.long_desc ReferralDoctorSpecialityDesc,
  c.ACCESSION_NUM,
  nt.NOTE_TYPE_DESC,
  e.facility_id,
  authDoc.PRACTITIONER_NAME as AUTHORISEDDOCNAME,
  TO_CHAR(c.modified_date, 'YYYYMMDDHH24MISS') as AUTHORISEDDATE,
  CASE
  WHEN e.ASSIGN_CARE_LOCN_TYPE ='N'
  THEN (SELECT LONG_DESC FROM IP_NURSING_UNIT
          WHERE FACILITY_ID = E.FACILITY_ID AND NURSING_UNIT_CODE = E.ASSIGN_CARE_LOCN_CODE )
  WHEN e.ASSIGN_CARE_LOCN_TYPE = 'C'
  THEN (SELECT LONG_DESC FROM OP_CLINIC
          WHERE FACILITY_ID = E.FACILITY_ID AND CLINIC_CODE =  E.ASSIGN_CARE_LOCN_CODE )
  ELSE ''
  END Location,
  CASE
  WHEN c.EXT_IMAGE_APPL_ID IS NULL
  THEN (SELECT HIST_DATA Note_Content FROM CR_ENCOUNTER_DETAIL_TXT
          WHERE ACCESSION_NUM = c.ACCESSION_NUM AND CONTR_SYS_ID = 'CA')
  ELSE NULL
  END Note_Content,
  
  CASE
  WHEN c.EXT_IMAGE_APPL_ID IS NULL
  THEN (SELECT cens.NOTES_SECTION_CONTENT XMLCONTENT FROM CA_ENCNTR_NOTE_SECTION
          WHERE ACCESSION_NUM = c.ACCESSION_NUM AND SEC_HDG_CODE = 'HDC')
  ELSE NULL
  END XMLCONTENT
  
FROM
  CR_ENCOUNTER_DETAIL c,
  PR_Encounter e,
  CR_ENCOUNTER_DETAIL_OBJ co,
  am_practitioner pr, CA_NOTE_TYPE nt,am_speciality spec,MPHRX_MPPT_VW p,
  am_practitioner authDoc,
  CA_ENCNTR_NOTE_SECTION cens

WHERE
c.ENCOUNTER_ID = e.ENCOUNTER_ID
and c.FACILITY_ID = e.FACILITY_ID
and c.PATIENT_ID = p.PATIENT_ID
-- AND NVL(c.status, 'N/A') not in ('E')
AND co.ACCESSION_NUM(+) = c.accession_num and co.HIST_REC_TYPE(+) =c.HIST_REC_TYPE and co.CONTR_SYS_ID(+) = c.contr_sys_id and co.CONTR_SYS_EVENT_CODE(+) = c.CONTR_SYS_EVENT_CODE
AND c.accession_num(+) = cens.ACCESSION_NUM
AND pr.practitioner_id = e.ATTEND_PRACTITIONER_ID
AND nt.NOTE_TYPE  = c.CONTR_SYS_EVENT_CODE
AND spec.speciality_code(+) = pr.primary_speciality_code
AND authDoc.practitioner_id(+) = c.AUTHORIZED_BY_ID
AND ( 
(nt.NOTE_GROUP_ID = 'OPSUMMARY' and c.PATIENT_CLASS = 'OP')
OR (nt.NOTE_GROUP_ID = '*DISSUMMPH' and c.PATIENT_CLASS in ('IP', 'DC'))
OR (nt.NOTE_GROUP_ID ='D001')
OR (nt.NOTE_GROUP_ID ='D002')
)
--AND nt.NOTE_GROUP_ID ='D002'
--AND c.EXT_IMAGE_APPL_ID is null
--AND c.ACCESSION_NUM = 'CN17\\\\\\\$0000000005540276'
--and c.PATIENT_ID in ('MM00000109','MM01291430','MM00000109')
--and co.OBJTYPE = 'image/jpeg'
--AND c.patient_class = 'IP'
--and c.ENCOUNTER_ID = '12552585'
--AND rownum < 5 """;
        return Query
    }

public String opdNotesUnionSqlQuery(){
        String Query = """
                UNION ALL
                SELECT
  nt.NOTE_GROUP_ID NOTEGROUP,
  p.patient_id RegistrationNo,
  NVL(p.contact1_no,p.contact2_no) PatientPhoneNo,
  c.status documentStatus,
  p.EMAIL_ID PatientEmailAddress,
  p.first_name FirstName,
  p.family_name LastName,
  p.second_name middleName,
  p.name_prefix patientTitle,
  p.sex PatientGender,
  TO_CHAR(p.date_of_birth , 'yyyymmdd') DateofBirth,
  c.patient_class InPatientOrOut,
  c.encounter_id EncounterId,
  TO_CHAR(c.event_date, 'YYYYMMDDHH24MISS') msgOriginationDate,
  TO_CHAR(e.VISIT_ADM_DATE_TIME, 'YYYYMMDDHH24MISS') VISITDATE,
  TO_CHAR(e.DISCHARGE_DATE_TIME, 'YYYYMMDDHH24MISS') dischargeDate,
  c.EXT_IMAGE_APPL_ID,
  co.EVENT_TITLE,
  nvl(co.FILE_NAME,(case
when c.EXT_IMAGE_APPL_ID is not null and co.FILE_NAME is null then
(select obj.FILE_NAME from or_result_Detail ord , IBAEHIS.cr_encounter_detail_obj obj
where ord.order_type_code = substr(c.accession_num,1,4) and ord.order_id = substr(c.accession_num,6,15) and ord.report_srl_no = substr(c.accession_num,22,1) and ord.line_srl_no=substr(c.accession_num,24,1) and ord.srl_no=substr(c.accession_num,26,1)
and obj.accession_num = ord.linked_note_accession_num and obj.contr_sys_event_code = ord.discr_msr_panel_id and obj.contr_sys_id = 'CA' and obj.hist_rec_type = 'CLNT')
END)) as FILE_NAME,
  nvl(co.HIST_DATA,(case
when c.EXT_IMAGE_APPL_ID is not null and co.hist_data is null then
(select obj.hist_data from or_result_Detail ord , IBAEHIS.cr_encounter_detail_obj obj
where ord.order_type_code = substr(c.accession_num,1,4) and ord.order_id = substr(c.accession_num,6,15) and ord.report_srl_no = substr(c.accession_num,22,1) and ord.line_srl_no=substr(c.accession_num,24,1) and ord.srl_no=substr(c.accession_num,26,1)
and obj.accession_num = ord.linked_note_accession_num and obj.contr_sys_event_code = ord.discr_msr_panel_id and obj.contr_sys_id = 'CA' and obj.hist_rec_type = 'CLNT')
END)) as HIST_DATA,
  nvl(co.OBJTYPE,(case
when c.EXT_IMAGE_APPL_ID is not null and co.OBJTYPE is null then
(select obj.OBJTYPE from or_result_Detail ord , IBAEHIS.cr_encounter_detail_obj obj
where ord.order_type_code = substr(c.accession_num,1,4) and ord.order_id = substr(c.accession_num,6,15) and ord.report_srl_no = substr(c.accession_num,22,1) and ord.line_srl_no=substr(c.accession_num,24,1) and ord.srl_no=substr(c.accession_num,26,1)
and obj.accession_num = ord.linked_note_accession_num and obj.contr_sys_event_code = ord.discr_msr_panel_id and obj.contr_sys_id = 'CA' and obj.hist_rec_type = 'CLNT')
END)) as OBJTYPE,
  e.ATTEND_PRACTITIONER_ID RefDoctorId,
  pr.practitioner_name ReferralDoctorName,
  spec.long_desc ReferralDoctorSpecialityDesc,
  c.ACCESSION_NUM,
  nt.NOTE_TYPE_DESC,
  e.facility_id,
  authDoc.PRACTITIONER_NAME as AUTHORISEDDOCNAME,
  TO_CHAR(c.modified_date, 'YYYYMMDDHH24MISS') as AUTHORISEDDATE,
  
  CASE
  WHEN e.ASSIGN_CARE_LOCN_TYPE ='N'
  THEN (SELECT LONG_DESC FROM IP_NURSING_UNIT
          WHERE FACILITY_ID = E.FACILITY_ID AND NURSING_UNIT_CODE = E.ASSIGN_CARE_LOCN_CODE )
  WHEN e.ASSIGN_CARE_LOCN_TYPE = 'C'
  THEN (SELECT LONG_DESC FROM OP_CLINIC
          WHERE FACILITY_ID = E.FACILITY_ID AND CLINIC_CODE =  E.ASSIGN_CARE_LOCN_CODE )
  ELSE ''
  END Location,

  CASE
  WHEN c.EXT_IMAGE_APPL_ID IS NULL
  THEN (SELECT HIST_DATA Note_Content FROM CR_ENCOUNTER_DETAIL_TXT
          WHERE ACCESSION_NUM = c.ACCESSION_NUM AND CONTR_SYS_ID IN ('CA', 'OR'))
  ELSE NULL
  END Note_Content,
  NULL AS XMLCONTENT	

FROM
  CR_ENCOUNTER_DETAIL c,
  PR_Encounter e,
  CR_ENCOUNTER_DETAIL_OBJ co,
  am_practitioner pr, SMSNEW.EMR_NOTE_INTEGRATION nt, am_speciality spec,MPHRX_MPPT_VW p,
  am_practitioner authDoc

WHERE
c.ENCOUNTER_ID = e.ENCOUNTER_ID
and c.FACILITY_ID = e.FACILITY_ID
and c.PATIENT_ID = p.PATIENT_ID
-- AND NVL(c.status, 'N/A') not in ('E')
AND co.ACCESSION_NUM(+) = nvl(c.ext_image_obj_id,c.accession_num) and co.hist_rec_type(+) = c.hist_rec_type and co.contr_sys_id(+) = c.contr_sys_id and co.contr_sys_event_code(+) = c.contr_sys_event_code
AND pr.practitioner_id = e.ATTEND_PRACTITIONER_ID
AND nt.NOTE_TYPE  = c.CONTR_SYS_EVENT_CODE
AND spec.speciality_code(+) = pr.primary_speciality_code
AND authDoc.practitioner_id(+) = c.AUTHORIZED_BY_ID
--AND c.modified_date BETWEEN to_date('2020-05-20 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND to_date('2020-05-27 23:59:59','YYYY-MM-DD HH24:MI:SS')
--AND c.EXT_IMAGE_APPL_ID is null
--AND c.ACCESSION_NUM = 'E2926249' -- ECLINIIC 'HP29707' -- EMR 'E2926249'
--and c.PATIENT_ID = 'MM00009876';
--and co.OBJTYPE = 'image/jpeg'
--AND c.patient_class = 'OP'
--and c.ENCOUNTER_ID = '163032110001'; """;
        return Query

    }

    public def getFileFromBlob(def row,String FileStorageLocation,boolean isDocNo = false)
    {

        def OBJTYPE    = row.OBJTYPE
        def HIST_DATA = row.HIST_DATA
        def EVENT_TITLE = row.EVENT_TITLE
        def FILE_NAME = row.FILE_NAME
        def ACCESSION_NUM =  row.ACCESSION_NUM
        def ENCOUNTERID = row.ENCOUNTERID
        String encounterStartDate = row.VISITDATE
        def documentNumber = ""
        def documentFileName = ""
        def fileLocation = ""
        //Assumming that mime type would be application/pdf

        if(HIST_DATA && (OBJTYPE == 'application/pdf' || OBJTYPE == 'text/plain' || OBJTYPE == 'image/x-png' || OBJTYPE ==  'image/pjpeg' || OBJTYPE ==  'image/gif' || OBJTYPE ==  'image/bmp' || OBJTYPE ==  'image/tiff' || OBJTYPE ==  'image/jpeg' || OBJTYPE ==  'image/png'))
        {
            if(encounterStartDate && encounterStartDate != null && encounterStartDate != "")
            {
                String year = encounterStartDate.substring(0,4);
                String month = encounterStartDate.substring(4,6);
                String day = encounterStartDate.substring(6,8);
                if(year && month && day)
                {
                    fileLocation = '/'+year+'/'+month+'/'+day;
                }
            }
            if(ENCOUNTERID) {
                documentNumber = ENCOUNTERID
                fileLocation = fileLocation+'/'+ENCOUNTERID
            }
            if(ACCESSION_NUM)
                documentNumber = documentNumber+"_"+ACCESSION_NUM
            if(FILE_NAME)
                documentNumber = documentNumber+"_"+FILE_NAME

            documentNumber = documentNumber.replaceAll(/\./,'')
            documentNumber = documentNumber.replaceAll(/\//,'')
            documentNumber = documentNumber.replaceAll(/\$/,'_')
            documentNumber = documentNumber.replaceAll(/\s+/,'_')
            documentNumber = documentNumber.replaceAll(/\W/,'_')

            if(isDocNo == true)
                return [documentNumber]

            if(OBJTYPE == 'application/pdf')
                documentFileName = documentNumber+".pdf"
            if(OBJTYPE == 'text/plain')
                documentFileName = documentNumber+".txt"
            if(OBJTYPE == 'image/x-png')
                documentFileName = documentNumber+".png"
            if(OBJTYPE == 'image/pjpeg')
                documentFileName = documentNumber+".jpg"
            if(OBJTYPE == 'image/gif')
                documentFileName = documentNumber+".gif"
            if(OBJTYPE == 'image/bmp')
                documentFileName = documentNumber+".bmp"
            if(OBJTYPE == 'image/tiff')
                documentFileName = documentNumber+".tif"
            if(OBJTYPE == 'image/jpeg')
                documentFileName = documentNumber+".jpg"
            if(OBJTYPE == 'image/png')
                documentFileName = documentNumber+".png"

            fileLocation = FileStorageLocation+fileLocation+"/"
            File path = new File(fileLocation)
            if(!(path.exists()))
            {
                path.mkdirs()
            }
            fileLocation = fileLocation+documentFileName
            // Open a Stream for creating a new file
            convertBlobToFile(fileLocation,HIST_DATA)

        }

        if(isDocNo == true){
            log.error("[Message: ]: Error: Returning null document Number ${documentNumber} as the OBJTYPE or HIST_DATA criteria is not fulfilled!!!!!!")
            return [documentNumber]
        }else{
            return [documentNumber,documentFileName,fileLocation]
        }


    }

    public def convertBlobToFile(def fileLocation,def HIST_DATA)
    {
        File   blobFile   = new File(fileLocation);
        //remove file if exist
        if(blobFile.exists())
        {
            blobFile.delete()
        }

        FileOutputStream  outStream  = new FileOutputStream(blobFile);
        InputStream       inStream   = HIST_DATA.getBinaryStream();

        int     length  = -1;
        int     size    = HIST_DATA.getBufferSize();
        byte[]  buffer  = new byte[size];

        while ((length = inStream.read(buffer)) != -1)
        {
            outStream.write(buffer, 0, length);
            outStream.flush();
        }
        inStream.close();
        outStream.close();
    }

    def getPdfFromXml (row)
    {
        def XML_CONTENT = row.XMLCONTENT

        def xmlDataObject
        String encounterStartDate = row.VISITDATE
        def documentNumber = ""
        def documentFileName = ""
        def fileLocation = ""
        if(XML_CONTENT) {
            def XML_CONTENT_STR = XML_CONTENT.getSubString(1, (int)XML_CONTENT.length())
            xmlDataObject = xmlParserService.extractDataFromXmlFile(XML_CONTENT_STR.toString())
            String header = createHeaderForPdfHhr(row)
            String footer = createFooterForPdfHhr(row)
            File headerFile =           makeTmpForHeadFootFile(header)
            File footerFile =           makeTmpForHeadFootFile(footer)
            //wrapper.headerHtml =        headerFile.absolutePath
            //wrapper.footerHtml =        footerFile.absolutePath
			Map headFoot = [:]
            headFoot.put("header",header)
            headFoot.put("footer",footer)
			fileLocation = xmlParserService.createPDFWithXmlData(xmlDataObject, headFoot )

            // File path = new File(fileLocation)
            if(fileLocation){
				(documentNumber, documentFileName) = getDocumentDetail(row);
            }
        }
        return [documentNumber, documentFileName, fileLocation]
    }

	def getPdfFromDcsXml (def row)
    {
		log.info("Inside getPdfFromDcsXml");
		def XML_CONTENT = row.DCS_XMLCONTENT
		String documentNumber = "";
		String documentFileName = "";
		String fileLocation = "";
		try{
			if(XML_CONTENT) {
				String XML_CONTENT_STR = XML_CONTENT.getSubString(1, (int) XML_CONTENT.length());
				Map dischargeSummaryMap = new HashMap();

				dischargeSummaryMap.putAll(getDischargeSummaryDetail(row));
				dischargeSummaryMap.putAll(extractDataFromDcsXmlFile(XML_CONTENT_STR.toString()));

				File file = new File(configObject.chestSurgeryHtmlPath);
				String fileContent = file.text;
				fileContent = replaceVarFromString(fileContent, dischargeSummaryMap);

				(documentNumber,documentFileName,fileLocation) = getPdfFromHtml(row, configObject.dcsStoragePath, false,fileContent, false, true);
			}else{
				log.info("getPdfFromDcsXml : XML_CONTENT is empty : ${XML_CONTENT}")
			}
		}
		catch(ex){
			log.error("getPdfFromDcsXml : Exception occured while generating PDF for row : ${row} ",ex);
		}

		log.info("getPdfFromDcsXml : return documentNumber : ${documentNumber},  documentFileName : ${documentFileName}, fileLocation : ${fileLocation}");
        return [documentNumber, documentFileName, fileLocation];
    }

	String replaceVarFromString(String data, Map varMap){
		log.info("Inside replaceVarFromString");
		for(i in varMap){
			String search = "\${${i.key}}";
			String replace = i.value ?: "";
			data = data.replace(search, replace);
		}
		return data;
	}

	def getDocumentDetail(def row){
		log.info("Inside getDocumentDetail");
		String documentNumber = "";
		String documentFileName = "";
		if(row.ENCOUNTERID){
			documentNumber = row.ENCOUNTERID
		}
		if(row.ACCESSION_NUM)
			documentNumber = documentNumber+"_"+row.ACCESSION_NUM

		documentNumber = documentNumber.replaceAll(/\./,'')
		documentNumber = documentNumber.replaceAll(/\//,'')
		documentNumber = documentNumber.replaceAll(/\$/,'_')
		documentNumber = documentNumber.replaceAll(/\s+/,'_')

		documentFileName = documentNumber + ".pdf";
		log.info("getDocumentDetail : return documentNumber : ${documentNumber},  documentFileName : ${documentFileName}");
		return [documentNumber, documentFileName];
	}

	Map getDischargeSummaryDetail(def row){
		log.info("Inside getDischargeSummaryDetail");
		log.debug("getDischargeSummaryDetail : input data : ${row}");
		Map detailMap = new HashMap();

		String patientName = ""
		if(row.PATIENTTITLE)
			patientName = row.PATIENTTITLE
		if(row.FIRSTNAME)
			patientName = patientName+" "+row.FIRSTNAME
		if(row.MIDDLENAME)
			patientName = patientName+" "+row.MIDDLENAME
		if(row.LASTNAME)
			patientName = patientName+" "+row.LASTNAME

		String gender = "Other"
		if(row.PATIENTGENDER == "M")
			gender="Male"
		if(row.PATIENTGENDER == "F")
			gender="Female"

		String age = "-"
		if(row.DATEOFBIRTH) {
			def dateOfBirth = DateUtils.parsedISO8601Date(row.DATEOFBIRTH)
			def patientAge = DateUtils.getAgeInYears(dateOfBirth)
			if(patientAge <= 1)
				age = "${patientAge}Y"
			else
				age = "${patientAge}Y"
		}

		String patientClass = ""
		if(row.INPATIENTOROUT == "OP")
			patientClass = "Outpatient"
		else if(row.INPATIENTOROUT == "IP")
			patientClass = "Inpatient"
		else
			patientClass = row.INPATIENTOROUT

		String dateCreate = "-"
		if(row.VISITDATE) {
			dateCreate = getDateString(row.VISITDATE,"yyyyMMddHHmmss","dd/MM/yyyy HH:mm")
		}

		detailMap.put("patientId", row.REGISTRATIONNO);
		detailMap.put("patientName", patientName);
		detailMap.put("gender", gender);
		detailMap.put("age", age);
		detailMap.put("encounterId", (row.ENCOUNTERID ?: ""));
		detailMap.put("patientClass", patientClass);
		detailMap.put("dateCreate", dateCreate);
		detailMap.put("location", (row.LOCATION ?: "-"));
		detailMap.put("speciality", row.REFERRALDOCTORSPECIALITYDESC);
		detailMap.put("attndPractitioner", row.REFERRALDOCTORNAME);
		detailMap.put("dischargeDate", (row.DISCHARGEDATE ? getDateString(row.DISCHARGEDATE,"yyyyMMddHHmmss","dd/MM/yyyy HH:mm") : "-"));

		log.info("getDischargeSummaryDetail : Patient discharge details : ${detailMap}")
		return detailMap
	}

    public def getPdfFromHtml(def row,String FileStorageLocation,Boolean isDocNo = false, String htmlString = "", Boolean headerRequired = true, Boolean footerRequired = true) {
        def NOTE_CONTENT = htmlString ?: row.NOTE_CONTENT;
        String encounterStartDate = row.VISITDATE
        def documentNumber = ""
        def documentFileName = ""
        def fileLocation = ""

        if(NOTE_CONTENT) {
            if(encounterStartDate && encounterStartDate != null && encounterStartDate != "")
            {
                String year = encounterStartDate.substring(0,4);
                String month = encounterStartDate.substring(4,6);
                String day = encounterStartDate.substring(6,8);
                if(year && month && day)
                {
                    fileLocation = '/'+year+'/'+month+'/'+day;
                }
            }
            if(row.ENCOUNTERID) {
                fileLocation = fileLocation+'/'+row.ENCOUNTERID
            }
			fileLocation = FileStorageLocation+fileLocation+"/";
			(documentNumber, documentFileName) = getDocumentDetail(row);

            if(isDocNo == true)
                return [documentNumber]

            File path = new File(fileLocation)
            if(!(path.exists()))
            {
                path.mkdirs()
            }

            WkhtmltoxWrapper wrapper = new WkhtmltoxWrapper()
            wrapper.marginLeft = 25
			wrapper.marginTop =  headerRequired ? 140 : 25;
            wrapper.marginBottom = 75
            wrapper.marginRight = 25
            wrapper.headerSpacing = 10
            wrapper.footerSpacing = 25
            wrapper.disableSmartShrinking = true
            wrapper.pageHeight = 594
            wrapper.pageWidth = 420
            wrapper.encoding = "UTF-8"

			File headerFile = null;
			File footerFile = null;
			if(headerRequired) {
				String header = createHeaderForPdf(row);
				headerFile = makeTmpForHeadFootFile(header);
				wrapper.headerHtml = headerFile.absolutePath;
			}
			if(footerRequired){
				String footer = createFooterForPdf(row);
				footerFile = makeTmpForHeadFootFile(footer);
				wrapper.footerHtml = footerFile.absolutePath;
			}

            def wkhtmltoxConfig = grailsApplication.mergedConfig.grails.plugin.wkhtmltox

            String binaryFilePath = wkhtmltoxConfig.binary.toString()
            if (!(new File(binaryFilePath)).exists()) {
                println "Cannot find wkhtml executable at $binaryFilePath trying to make it available with the makeBinaryAvailableClosure"
                Closure makeBinaryAvailableClosure = wkhtmltoxConfig.makeBinaryAvailableClosure
                makeBinaryAvailableClosure.call(binaryFilePath)
            }

            //def byte[] pdfData = new WkhtmltoxExecutor(binaryFilePath,wrapper).generatePdf(NOTE_CONTENT?.asciiStream.text.toString())

			if(!htmlString){
				htmlString = NOTE_CONTENT.getSubString(1, (int)NOTE_CONTENT.length())
				//replacing all mandatory field image with *
				htmlString = htmlString.replaceAll('<IMAGE SRC="../../eCommon/images/mandatory.gif"></IMAGE>','<b style="color:red !important">*</b>')
				//Appending css file to handle italic tags
				htmlString = '<link href="'+cssPath+'css/reportPdfContent.css" rel="stylesheet" /><div>'+htmlString+'</div>';
			}

            def byte[] pdfData = new WkhtmltoxExecutor(binaryFilePath,wrapper).generatePdf(htmlString.toString())

			if(headerFile && headerFile.exists())
				headerFile.delete();
			if(footerFile && footerFile.exists())
				footerFile.delete();

            if(pdfData)
            {
                //htmlFile.delete()
                fileLocation = fileLocation+documentNumber+".pdf"
                documentFileName = documentNumber+".pdf"
                File pdfFile  = new File(fileLocation);
                pdfFile.bytes = pdfData
            }
            else
            {
                documentNumber = ""
                documentFileName = ""
                fileLocation = ""
                //htmlFile.delete()
                log.error("[Message: MedantaOpdNotesCustomJobService]: Error: PDF has not been generated for html file !!!!!!")
            }
        }

        if(isDocNo == true){
            log.error("[Message:]: Error: Returning null document Number ${documentNumber} as the NOTE_CONTENT criteria is not fulfilled!!!!!!")
            return [documentNumber]
        }else{
            return [documentNumber,documentFileName,fileLocation]
        }
    }

    public File makeTmpForHeadFootFile(String content){
        File tempFile = File.createTempFile("/wkhtmltopdf",".html")
        tempFile.withWriter("UTF8") {
            it.write(content)
            it.close()
        }
        tempFile.setReadable(true,true)
        tempFile.setWritable(true,true)
        return tempFile
    }

   String createHeaderForPdf(def row)
    {
        String header = '<!DOCTYPE html><html><link href="'+cssPath+'css/reportHeadFoot.css" rel="stylesheet" /><div><img src="'+cssPath+'images/mphrx_logo.png"></div>'
        header = header+'<div style ="color:#333;font-weight:700;font-size:25px;text-align:center;margin-top:-32px">Medanta</div><div style ="border: 2px solid gray;padding: 10px;margin: 10px;"><table class="reportTableHeader"><tr>';
        header = header+'<td><b>Patient ID</b></td><td>:  '+row.REGISTRATIONNO+'</td>'

        String patientName = ""
        if(row.PATIENTTITLE)
            patientName = row.PATIENTTITLE
        if(row.FIRSTNAME)
            patientName = patientName+" "+row.FIRSTNAME
        if(row.MIDDLENAME)
            patientName = patientName+" "+row.MIDDLENAME
        if(row.LASTNAME)
            patientName = patientName+" "+row.LASTNAME

        header = header+'<td><b>Patient Name</b></td><td>:  '+patientName+'</td></tr>'
        String gender = "Other"
        if(row.PATIENTGENDER == "M")
            gender="Male"
        if(row.PATIENTGENDER == "F")
            gender="Female"
        header = header+'<tr><td><b>Gender</b></td><td>:  '+gender+'</td>'
        String age = "-"
        if(row.DATEOFBIRTH) {
            def dateOfBirth = DateUtils.parsedISO8601Date(row.DATEOFBIRTH)
            def patientAge = DateUtils.getAgeInYears(dateOfBirth)
            if(patientAge <= 1)
                age = "${patientAge}Y"
			else
                age = "${patientAge}Y"
        }
        header = header+'<td><b>Age</b></td><td>:  '+age+' </td></tr>'

        header = header+'<tr><td><b>Encounter ID</b></td><td>:  '+row.ENCOUNTERID+' </td>'
        String patientClass = ""
        String visitDate = "Visit Date"
        if(row.INPATIENTOROUT == "OP") {
            patientClass = "Outpatient"
        }
        else if(row.INPATIENTOROUT == "IP") {
            patientClass = "Inpatient"
            visitDate = "Admission Date"
        }
        else
            patientClass = row.INPATIENTOROUT
        header = header + '<td><b>Encounter Type</b></td><td>:  ' + patientClass + ' </td></tr>'

        if(row.VISITDATE) {
            def dateCreate = getDateString(row.VISITDATE,"yyyyMMddHHmmss","dd/MM/yyyy HH:mm")
            header = header + '<tr><td><b>'+visitDate+'</b></td><td>:  ' + dateCreate + ' </td>'
        }
        else
        {
            header = header + '<tr><td><b>'+visitDate+'</b></td><td>:   - </td>'
        }
        if(row.LOCATION)
            header = header+'<td><b>Location</b></td><td>:  '+row.LOCATION+'  </td></tr>'
        else
            header = header+'<td><b>Location</b></td><td>:   -  </td></tr>'
        header = header+'<tr><td><b>Speciality</b></td><td>:  '+row.REFERRALDOCTORSPECIALITYDESC+' </td><td><b>Attending Practitioner</b></td><td>:  '+row.REFERRALDOCTORNAME+'  </td></tr>'
        if(row.DISCHARGEDATE && row.INPATIENTOROUT == "IP")
        {
            def dischargeDate = getDateString(row.DISCHARGEDATE,"yyyyMMddHHmmss","dd/MM/yyyy HH:mm")
            header = header + '<tr><td><b>Discharge Date</b></td><td>:  '+dischargeDate+' </td><td></td></tr>'
        }
        header = header + '</table></div></html>'

        return header
    }

	String normalTelephoneAndFaxAddressLabel(){
		String content = '''
				<div style="clear:both;"></div>
                            <div style="display:inline;text-align:left;"><span
                                    data-ng-bind="&quot;reportingEngine.footerPage.addressLabelTelephone&quot; | translate"
                                    class="ng-binding">Tel: +91&nbsp;11&nbsp;4411&nbsp;4411</span></div>
                            <div style="display:inline;text-align:right;"><span style="float:right;"
                                                                                data-ng-bind="&quot;reportingEngine.footerPage.addressLabelFax&quot; | translate"
                                                                                class="ng-binding">Fax: +91&nbsp;11&nbsp;2433&nbsp;1433</span></div>
				'''
		return content;
	}

	String luckhnowHotlineLabel(){
		String content = '''
					<span ng-if="performingLocationId == ('reportingEngine.footerPage.lucknowCode' | translate)"
								  data-ng-bind="&quot;reportingEngine.footerPage.lucknow.hotLineLabel&quot; | translate"
								  class="ng-binding">24X7 hot-line: +91(522)4505050</span>
				'''
		return content;
	}

	String normalEmergencyLabel(){
		String content = '''<span
                                    data-ng-bind="&quot;reportingEngine.footerPage.emergencyLabel&quot; | translate"
                                    class="ng-binding">Emergency: 1068</span>
				'''
		return content;
	}
	String normalEmailLabel(){
		String content = '''<span
                                    data-ng-bind="&quot;reportingEngine.footerPage.emailLabel&quot; | translate"
                                    class="ng-binding">Email: info@medanta.org</span>
			'''

		return content;
	}
	String luckhnowCinCode(){
		String content = '''
				<span style="float:right;" ng-if="(performingLocationId == ('reportingEngine.footerPage.lucknowCode' | translate))"
                                      data-ng-bind="&quot;reportingEngine.footerPage.lucknow.cin&quot; | translate"
                                      class="ng-binding">CIN: U74140DL2013PTC250579</span>
			'''
		return content;
	}
	String websitLabel(){
		String content = '''
					<span
                                    data-ng-bind="&quot;reportingEngine.footerPage.websiteLabel&quot; | translate"
                                    class="ng-binding">www.medanta.org</span>
				'''
		return content;
	}

	String patnaHotLine(){
		String content = '''
				<span ng-if="performingLocationId == ('reportingEngine.footerPage.patnaCode' | translate)"
                              data-ng-bind="&quot;reportingEngine.footerPage.patna.hotLineLabel&quot; | translate"
                              class="ng-binding">24X7 help-line: +91 82922 22333</span>
				'''
		return  content;
	}

	String createFooterForPdf(def row)
	{
		log.info("====================FACILITY IN OPDDUMP="+row.FACILITY_ID)
		def authDate = ""
		if(row.AUTHORISEDDATE)
			authDate = getDateString(row.AUTHORISEDDATE,"yyyyMMddHHmmss","dd MMM yyyy HH:mm")
		String authFoother = '<div class="repFooter1"><table><tbody><tr>';
		if(row.AUTHORISEDDOCNAME)
			authFoother = authFoother+ '<td>Authorized by '+row.AUTHORISEDDOCNAME+' on '+authDate+'</td>'
		authFoother = authFoother+'<td style="text-align:right;">This is a computer generated report. Signature is not required.</td></tr></tbody></table></div>';

		//Earlier used for dark line     <div style="border: 1px solid #333;margin: 0 0 10px 0;">
		String footer = "";
		if(row.FACILITY_ID == "ML"){
			log.info("====================FACILITY IN OPDDUMP IS OF lucknow==="+row.FACILITY_ID)
			footer = '''<html><link href="'''+cssPath+'''css/reportHeadFoot.css" rel="stylesheet" /></div>

    '''+authFoother+'''
    <div style="font-weight:700; font-size:14px; line-height:16px;font-weight: bold;" class="repFooter2" >
        
	        <div>
            <table style="line-height:20px;">
                <tbody>
                <tr>
                    <td style="width:68% !important;">
                        <div style="display:inline;width:100% ;font-weight: bold;">
                            <div><span
                                    ng-if="(performingLocationId == ('reportingEngine.footerPage.lucknowCode' | translate))"
                                    data-ng-bind="&quot;reportingEngine.footerPage.lucknow.addressLabel&quot; | translate"
                                    class="ng-binding">Regd. Office: Medanta Holdings Private Limited, E-18 Defence colony , New Delhi, 110024, India.</span></div>
                        	</div>
                    </td>
                    <td style="width:1% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:none; display:inline-block;"></span></td>
                    <td style="width:45% !important;">
                        <div style="display:inline;width:100% ;font-weight: bold;">
                            '''+normalTelephoneAndFaxAddressLabel()+'''
                        </div>
                    </td>
                </tr>
                </tbody>
            </table>
        </div>

        <div>
            <table style="line-height:20px;">
                <tbody>
                <tr>
                    <td style="width:23% !important;font-weight: bold;">
                        '''+luckhnowHotlineLabel()+'''
                    </td>
                    <td style="width:1.5% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:13.5% !important;font-weight: bold;">
                        <div style="display:inline-block;width:100%">
                            <b style="float:left;margin-right:15px; ">'''+normalEmergencyLabel()+'''</b>
                        </div>
                    </td>
                    <td style="width:2% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:18.5% !important; text-align:right;">
                    	<div style="display:inline;width:100%">
                            <b style="float:left; ">'''+normalEmailLabel()+'''</b>
                        </div>

                    </td>
                    <td style="width:2% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:15.5% !important;">
                        <div style="">
                            <b  class="red">'''+websitLabel()+'''</b>
                        </div>
                    </td>
                    <td style="width:2% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:27.5% !important;">
                        <div class="text-right">
                            <b >
                                '''+luckhnowCinCode()+'''</b>
                        </div>
                    </td>
                </tr>
                </tbody>
            </table>
        </div>
		
</div></html>
'''
        }else if(row.FACILITY_ID == "UL"){
			log.info("====================FACILITY IN OPDDUMP IF DIFFERENT lucknowClinic==="+row.FACILITY_ID)
			footer = '''<html><link href="'''+cssPath+'''css/reportHeadFoot.css" rel="stylesheet" /></div>

    '''+authFoother+'''
    <div style="font-weight:700; font-size:14px; line-height:16px;font-weight: bold;" class="repFooter2">
        
	<div>
            <table style="line-height:20px;">
                <tbody>
                <tr>
                    <td style="width:63% !important;">
                        <div style="display:inline;width:100%;font-weight: bold;">
                            <div><span
                                    ng-if="(performingLocationId == ('reportingEngine.footerPage.gurugramCode' | translate))"
                                    data-ng-bind="&quot;reportingEngine.footerPage.gurugram.addressLabel&quot; | translate"
                                    class="ng-binding">Regd. Office: Global Health Limited, E-18 Defence colony , New Delhi, 110024, India</span></div>
                        </div>
                    </td>
                    <td style="width:2% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:none; display:inline-block;"></span></td>
                    <td style="width:49% !important;">
                        <div style="display:inline;width:100%;font-weight: bold;">
                            '''+normalTelephoneAndFaxAddressLabel()+'''
                        </div>
                    </td>
                </tr>
                </tbody>
            </table>
        </div>

        <div>
            <table style="line-height:20px;">
                <tbody>
                <tr>
                    <td style="width:23% !important;font-weight: bold;">
                        '''+luckhnowHotlineLabel()+'''
                    </td>
                    <td style="width:1.5% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:13.5% !important;font-weight: bold;">
                        <div style="display:inline-block;width:100%">
                            <b style="float:left;margin-right:15px; ">'''+normalEmergencyLabel()+'''</b>
                        </div>
                    </td>
                    <td style="width:2% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:18.5% !important; text-align:right;font-weight: bold;">
                    	<div style="display:inline;width:100%">
                            <b style="float:left; ">'''+normalEmailLabel()+'''</b>
                        </div>

                    </td>
                    <td style="width:2% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:15.5% !important;font-weight: bold;">
                        <div style="">
                            <b  class="red">'''+websitLabel()+'''</b>
                        </div>
                    </td>
                    <td style="width:2% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:27.5% !important;font-weight: bold;">
                        <div class="text-right">
                            <b >
                                <span style="float:right;" ng-if="(performingLocationId == ('reportingEngine.footerPage.gurugramCode' | translate))"
                                      data-ng-bind="&quot;reportingEngine.footerPage.gurugram.cin&quot; | translate"
                                      class="ng-binding">CIN: U85110DL2004PLC128319</span></b>
                        </div>
                    </td>
                </tr>
                </tbody>
            </table>
        </div>
		
</div></html>
'''
        }else if(row.FACILITY_ID == "BP" || row.FACILITY_ID == "PT"){
            log.info("====================FACILITY IN OPDDUMP IF DIFFERENT FROM Patna==="+row.FACILITY_ID)
			footer = '''<html><link href="'''+cssPath+'''css/reportHeadFoot.css" rel="stylesheet" /></div>

    '''+authFoother+'''
    <div style="font-weight:700; font-size:14px; line-height:16px;font-weight: bold;" class="repFooter2">
        
	<div>
            <table style="line-height:20px;">
                <tbody>
                <tr>
                    <td style="width:63% !important;">
                        <div style="display:inline;width:100%;font-weight: bold;">
                            <div><span
                                    ng-if="(performingLocationId == ('reportingEngine.footerPage.patnaCode' | translate))"
                                    data-ng-bind="&quot;reportingEngine.footerPage.patna.addressLabel&quot; | translate"
                                    class="ng-binding">Regd. Office: Global Health Patliputra Private Limited.</span></div>
                        </div>
                    </td>
                    <td style="width:2% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:none; display:inline-block;"></span></td>
                    <td style="width:49% !important;">
                        <div style="display:inline;width:100%;font-weight: bold;">
                            '''+normalTelephoneAndFaxAddressLabel()+'''
                        </div>
                    </td>
                </tr>
                </tbody>
            </table>
        </div>

        <div>
            <table style="line-height:20px;">
                <tbody>
                <tr>
                    <td style="width:23% !important;font-weight: bold;">                       
                        '''+patnaHotLine()+'''
                    </td>
                    <td style="width:1.5% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:13.5% !important;font-weight: bold;">
                        <div style="display:inline-block;width:100%">
                            <b style="float:left;margin-right:15px; ">'''+normalEmergencyLabel()+'''</b>
                        </div>
                    </td>
                    <td style="width:2% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:18.5% !important; text-align:right;font-weight: bold;">
                        <div style="display:inline;width:100%">
                            <b style="float:left; ">'''+normalEmailLabel()+'''</b>
                        </div>
                    </td>
                    <td style="width:2% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:15.5% !important;font-weight: bold;">
                        <div style="">
                            <b  class="red">'''+websitLabel()+'''</b>
                        </div>
                    </td>
                    <td style="width:2% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:27.5% !important;font-weight: bold;">
                        <div class="text-right">
                            <b >
                                <span style="float:right;" ng-if="(performingLocationId == ('reportingEngine.footerPage.patnaCode' | translate))"
                                      data-ng-bind="&quot;reportingEngine.footerPage.patna.cin&quot; | translate"
                                      class="ng-binding">CIN: U74999DL2015PTC283932</span>
                                </b>
                        </div>
                    </td>
                </tr>
                </tbody>
            </table>
        </div>
		
</div></html>
'''
        }else{
            log.info("====================FACILITY IN OPDDUMP IF DIFFERENT FROM LKO==="+row.FACILITY_ID)
		footer = '''<html><link href="'''+cssPath+'''css/reportHeadFoot.css" rel="stylesheet" /></div>
    '''+authFoother+'''
    <div style="font-weight:700; font-size:14px; line-height:16px;font-weight: bold;" class="repFooter2">
        
	<div>
            <table style="line-height:20px;">
                <tbody>
                <tr>
                    <td style="width:63% !important;">
                        <div style="display:inline;width:100%;font-weight: bold;">
                            <div><span
                                    ng-if="(performingLocationId == ('reportingEngine.footerPage.gurugramCode' | translate))"
                                    data-ng-bind="&quot;reportingEngine.footerPage.gurugram.addressLabel&quot; | translate"
                                    class="ng-binding">Regd. Office: Global Health Limited, E-18 Defence colony , New Delhi, 110024, India</span></div>
                        </div>
                    </td>
                    <td style="width:2% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:none; display:inline-block;"></span></td>
                    <td style="width:49% !important;">
                        <div style="display:inline;width:100% ;font-weight: bold;">
                            '''+normalTelephoneAndFaxAddressLabel()+'''
                        </div>
                    </td>
                </tr>
                </tbody>
            </table>
        </div>

        <div>
            <table style="line-height:20px;">
                <tbody>
                <tr>
                    <td style="width:23% !important;;font-weight: bold;">
                        <span ng-if="performingLocationId == ('reportingEngine.footerPage.gurugramCode' | translate)"
                              data-ng-bind="&quot;reportingEngine.footerPage.gurugram.hotLineLabel&quot; | translate"
                              class="ng-binding">24X7 hot-line: +91(124)4141414</span>
                    </td>
                    <td style="width:1.5% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:13.5% !important;font-weight: bold;">
                        <div style="display:inline-block;width:100%">
                            <b style="float:left;margin-right:15px; ">'''+normalEmergencyLabel()+'''</b>
                        </div>
                    </td>
                    <td style="width:2% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:18.5% !important; text-align:right;">
                        <div style="display:inline;width:100%">
                            <b style="float:left;">'''+normalEmailLabel()+'''</b>
                        </div>

                    </td>
                    <td style="width:2% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:15.5% !important;">
                        <div style="">
                            <b  class="red">'''+websitLabel()+'''</b>
                        </div>
                    </td>
                    <td style="width:2% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:27.5% !important;">
                        <div class="text-right">
                            <b >
                                <span style="float:right;" ng-if="(performingLocationId == ('reportingEngine.footerPage.gurugramCode' | translate))"
                                      data-ng-bind="&quot;reportingEngine.footerPage.gurugram.cin&quot; | translate"
                                      class="ng-binding">CIN: U85110DL2004PLC128319</span></b>
                        </div>
                    </td>
                </tr>
                </tbody>
            </table>
        </div>
		
</div></html>
'''
	}



		return footer;
	}

    public String createHeaderForPdfHhr(def row)
    {
        String header = '<!DOCTYPE html><html><head><link href="'+cssPath+'css/reportHeadFootHhr.css" rel="stylesheet" /></head><body><div class="pdfHeader"><div class="pdfHtitle"><img class="pdfLogo" src="'+cssPath+'images/medanta.png">'

        header = header+'<h1>Medanta</h1></div><div class="pdfHtext"><table><tr>';
        header = header+'<td><b>Patient ID</b></td><td>:  '+row.REGISTRATIONNO+'</td>'

        String patientName = ""
        if(row.PATIENTTITLE)
            patientName = row.PATIENTTITLE
        if(row.FIRSTNAME)
            patientName = patientName+" "+row.FIRSTNAME
        if(row.MIDDLENAME)
            patientName = patientName+" "+row.MIDDLENAME
        if(row.LASTNAME)
            patientName = patientName+" "+row.LASTNAME

        header = header+'<td><b>Patient Name</b></td><td>:  '+patientName+'</td></tr>'
        String gender = "Other"
        if(row.PATIENTGENDER == "M")
            gender="Male"
        if(row.PATIENTGENDER == "F")
            gender="Female"
        header = header+'<tr><td><b>Gender</b></td><td>:  '+gender+'</td>'
        String age = "-"
        if(row.DATEOFBIRTH) {
            def dateOfBirth = DateUtils.parsedISO8601Date(row.DATEOFBIRTH)
            def patientAge = DateUtils.getAgeInYears(dateOfBirth)
            if(patientAge <= 1)
                age = "${patientAge}Y"
            else
                age = "${patientAge}Y"
        }
        header = header+'<td><b>Age</b></td><td>:  '+age+' </td></tr>'
        header = header+'<tr><td><b>Encounter ID</b></td><td>:  '+row.ENCOUNTERID+' </td>'
        String patientClass = ""
        String visitDate = "Visit Date"
        if(row.INPATIENTOROUT == "OP") {
            patientClass = "Outpatient"
        }
        else if(row.INPATIENTOROUT == "IP") {
            patientClass = "Inpatient"
            visitDate = "Admission Date"
        }
        else
            patientClass = row.INPATIENTOROUT
        header = header + '<td><b>Encounter Type</b></td><td>:  ' + patientClass + ' </td></tr>'

        if(row.VISITDATE) {
            def dateCreate = getDateString(row.VISITDATE,"yyyyMMddHHmmss","dd/MM/yyyy HH:mm")
            header = header + '<tr><td><b>'+visitDate+'</b></td><td>:  ' + dateCreate + ' </td>'
        }
        else
        {
            header = header + '<tr><td><b>'+visitDate+'</b></td><td>:   - </td>'
        }
        if(row.LOCATION)
            header = header+'<td><b>Location</b></td><td>:  '+row.LOCATION+'  </td></tr>'
        else
            header = header+'<td><b>Location</b></td><td>:   -  </td></tr>'

        header = header+'<tr><td><b>Speciality</b></td><td>:  '+row.REFERRALDOCTORSPECIALITYDESC+' </td><td><b>Attending Practitioner</b></td><td>:  '+row.REFERRALDOCTORNAME+'  </td></tr>'
        if(row.DISCHARGEDATE && row.INPATIENTOROUT == "IP")
        {
            def dischargeDate = getDateString(row.DISCHARGEDATE,"yyyyMMddHHmmss","dd/MM/yyyy HH:mm")
            header = header + '<tr><td><b>Discharge Date</b></td><td>:  '+dischargeDate+' </td><td></td></tr>'
        }
        header = header + '</table></div></div></body></html>'

        return header
    }

	String hhrTelePhoneAndFaxLabel(){
		String content = """
					<div style="clear:both;"></div>
                            <div style="display:inline;text-align:left;font-weight:normal;"><span style="font-weight:normal;"
                                    data-ng-bind="&quot;reportingEngine.footerPage.addressLabelTelephone&quot; | translate"
                                    class="ng-binding">Tel: +91&nbsp;11&nbsp;4411&nbsp;4411</span></div>
                            <div style="display:inline;text-align:right;font-weight:normal;"><span style="float:right;font-weight:normal;"
                                                                                data-ng-bind="&quot;reportingEngine.footerPage.addressLabelFax&quot; | translate"
                                                                                class="ng-binding">Fax: +91&nbsp;11&nbsp;2433&nbsp;1433</span></div>
				"""
		return content;
	}

	String createFooterForPdfHhr(def row)
	{
		def authDate = ""
		if(row.AUTHORISEDDATE)
			authDate = getDateString(row.AUTHORISEDDATE,"yyyyMMddHHmmss","dd MMM yyyy HH:mm")
		String authFoother = '<div class="line1"><table><tbody><tr>';
		if(row.AUTHORISEDDOCNAME){
			authFoother = authFoother+ '<td>Authorized by '+row.AUTHORISEDDOCNAME+' on '+authDate+'</td>'
		}
		log.info("[The facility of opd Notes: {row.AUTHORISEDDATE}]")
		authFoother = authFoother+'<td class="text-right">This is a computer generated report. Signature is not required.</td></tr></tbody></table></div>';
		//Earlier used for dark line     <div style="border: 1px solid #333;margin: 0 0 10px 0;">
		String footer ="";
		if(row.FACILITY_ID == "ML"  ){
			log.info("====================FACILITY IN OPDDUMP is lucknow="+row.FACILITY_ID)
			footer = """<html><head><link href=\""""+cssPath+"""css/reportHeadFootHhr.css"  rel="stylesheet" /><script>
        function subst() {
            var vars={};
            var x=document.location.search.substring(1).split('&');
            for (var i in x) {var z=x[i].split('=',2);vars[z[0]] = unescape(z[1]);}
            var x=['frompage','topage','page','webpage','section','subsection','subsubsection'];
            for (var i in x) {
                var y = document.getElementsByClassName(x[i]);
                for (var j=0; j<y.length; ++j) y[j].textContent = vars[x[i]];
            }
        }
        </script>
</head>
<body onload="subst()"><div class="pdfFooter">
<div class="text-right"><b>Page No. </b> <span class="page"></span> of <span class="topage"></span></div>"""+authFoother+
					"""<div class="line2" style="font-weight: normal;">
        <div>
            <table style="line-height:20px;font-size:10px;font-weight:normal;">
                <tbody>
                <tr>
                    <td style="width:69% !important;">
                        <div style="display:inline;width:100%;font-weight:normal;">
                            <div style="font-weight:normal;"><span style="font-weight:normal;"
                                    ng-if="(performingLocationId == ('reportingEngine.footerPage.lucknowCode' | translate))"
                                    data-ng-bind="&quot;reportingEngine.footerPage.lucknow.addressLabel&quot; | translate"
                                    class="ng-binding">Regd. Office: Medanta Holdings Private Limited, E-18 Defence colony , New Delhi, 110024, India.</span></div>
                        </div>
                    </td>
                    <td style="width:31% !important;font-weight:normal;">
                        <div style="display:inline;width:100%;font-weight:normal;">
                            """+hhrTelePhoneAndFaxLabel()+"""
                        </div>
                    </td>
                </tr>
                </tbody>
            </table>
        </div>
<div>
            <table style="line-height:20px;font-size:10px;font-weight:normal; width: 100%">
                <tbody>
                <tr>
                    <td style="width:24% !important;font-weight:normal;">
                        <span style="font-weight:normal;" ng-if="performingLocationId == ('reportingEngine.footerPage.lucknowCode' | translate)"
                              data-ng-bind="&quot;reportingEngine.footerPage.lucknow.hotLineLabel&quot; | translate"
                              class="ng-binding">24X7 hot-line: +91(522)4505050</span>
                    </td>
                    <td style="width:1% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:auto !important;font-weight:normal;">
                        <div style="display:inline-block;width:100%font-weight:normal;">
                            <b style="float:left;margin-right:15px; font-weight: normal;">"""+normalEmergencyLabel()+"""</b>

                        </div>
                    </td>
                    <td style="width:1% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:19% !important; text-align:right;font-weight:normal;">
                        <div style="display:inline;width:100%font-weight:normal;">
                            <b style="float:left; font-weight: normal;">"""+normalEmailLabel()+"""</b>
                        </div>

                    </td>
                    <td style="width:1% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:auto !important;">
                        <div style="">
                            <b style="font-weight: normal;" class="red">"""+websitLabel()+"""</b>
                        </div>
                    </td>
                    <td style="width:1% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:23%!important;">
                        <div class="text-right">
                            <b style="font-weight: normal;">
                                """+luckhnowCinCode()+"""</b>
                        </div>
                    </td>
                </tr>
                </tbody>
            </table>
        </div>
    </div>
</div></body></html>
"""

		}else if(row.FACILITY_ID == "UL"){
			log.info("====================FACILITY IN OPDDUMP is lucknowClinic="+row.FACILITY_ID)
			footer = """<html><head><link href=\""""+cssPath+"""css/reportHeadFootHhr.css"  rel="stylesheet" /><script>
        function subst() {
            var vars={};
            var x=document.location.search.substring(1).split('&');
            for (var i in x) {var z=x[i].split('=',2);vars[z[0]] = unescape(z[1]);}
            var x=['frompage','topage','page','webpage','section','subsection','subsubsection'];
            for (var i in x) {
                var y = document.getElementsByClassName(x[i]);
                for (var j=0; j<y.length; ++j) y[j].textContent = vars[x[i]];
            }
        }
        </script>
</head>
<body onload="subst()"><div class="pdfFooter">
<div class="text-right"><b>Page No. </b> <span class="page"></span> of <span class="topage"></span></div>"""+authFoother+
					"""<div class="line2" style="font-weight: normal;">
        <div>
            <table style="line-height:20px;font-size:10px;font-weight:normal;">
                <tbody>
                <tr>
                    <td style="width:67% !important;">
                        <div style="display:inline;width:100%;font-weight:normal;">
                            <div style="font-weight:normal;"><span style="font-weight:normal;"
                                    ng-if="(performingLocationId == ('reportingEngine.footerPage.gurugramCode' | translate))"
                                    data-ng-bind="&quot;reportingEngine.footerPage.gurugram.addressLabel&quot; | translate"
                                    class="ng-binding">Regd. Office: Global Health Limited, E-18 Defence colony , New Delhi, 110024, India</span></div>                            
                        </div>
                    </td>
                    <td style="width:33% !important;font-weight:normal;">
                        <div style="display:inline;width:100%;font-weight:normal;">
                            """+hhrTelePhoneAndFaxLabel()+"""
                        </div>
                    </td>
                </tr>
                </tbody>
            </table>
        </div>
<div>
            <table style="line-height:20px;font-size:10px;font-weight:normal; width: 100%">
                <tbody>
                <tr>
                    <td style="width:24% !important;font-weight:normal;">
                        """+luckhnowHotlineLabel()+"""
                    </td>
                    <td style="width:1% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:auto !important;font-weight:normal;">
                        <div style="display:inline-block;width:100%font-weight:normal;">
                            <b style="float:left;margin-right:15px; font-weight: normal;">"""+normalEmergencyLabel()+"""</b>

                        </div>
                    </td>
                    <td style="width:1% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:19% !important; text-align:right;font-weight:normal;">
                        <div style="display:inline;width:100%font-weight:normal;">
                            <b style="float:left; font-weight: normal;">"""+normalEmailLabel()+"""</b>
                        </div>

                    </td>
                    <td style="width:1% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:auto !important;">
                        <div style="">
                            <b style="font-weight: normal;" class="red">"""+websitLabel()+"""</b>
                        </div>
                    </td>
                    <td style="width:1% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:23%!important;">
                        <div class="text-right">
                            <b style="font-weight: normal;">
                                <span style="float:right;" ng-if="(performingLocationId == ('reportingEngine.footerPage.gurugramCode' | translate))"
                                       data-ng-bind="&quot;reportingEngine.footerPage.gurugram.cin&quot; | translate"
                                      class="ng-binding">CIN: U85110DL2004PLC128319</span></b>
                        </div>
                    </td>
                </tr>
                </tbody>
            </table>
        </div>
    </div>
</div></body></html>
"""
		}else if(row.FACILITY_ID == "PT" || row.FACILITY_ID == "BP"){
			log.info("====================FACILITY IN OPDDUMP is PATNA="+row.FACILITY_ID)
			footer = """<html><head><link href=\""""+cssPath+"""css/reportHeadFootHhr.css"  rel="stylesheet" /><script>
        function subst() {
            var vars={};
            var x=document.location.search.substring(1).split('&');
            for (var i in x) {var z=x[i].split('=',2);vars[z[0]] = unescape(z[1]);}
            var x=['frompage','topage','page','webpage','section','subsection','subsubsection'];
            for (var i in x) {
                var y = document.getElementsByClassName(x[i]);
                for (var j=0; j<y.length; ++j) y[j].textContent = vars[x[i]];
            }
        }
        </script>
</head>
<body onload="subst()"><div class="pdfFooter">
<div class="text-right"><b>Page No. </b> <span class="page"></span> of <span class="topage"></span></div>"""+authFoother+
					"""<div class="line2" style="font-weight: normal;">
        <div>
            <table style="line-height:20px;font-size:10px;font-weight:normal;">
                <tbody>
                <tr>
                    <td style="width:67% !important;">
                        <div style="display:inline;width:100%;font-weight:normal;">
                            <div style="font-weight:normal;"><span style="font-weight:normal;"
                                    ng-if="(performingLocationId == ('reportingEngine.footerPage.patnaCode' | translate))"
                                    data-ng-bind="&quot;reportingEngine.footerPage.patna.addressLabel&quot; | translate"
                                    class="ng-binding">Regd. Office: Global Health Patliputra Private Limited.</span></div>
                        </div>
                    </td>
                    <td style="width:33% !important;font-weight:normal;">
                        <div style="display:inline;width:100%;font-weight:normal;">
                            """+hhrTelePhoneAndFaxLabel()+"""
                        </div>
                    </td>
                </tr>
                </tbody>
            </table>
        </div>
<div>
            <table style="line-height:20px;font-size:10px;font-weight:normal; width: 100%">
                <tbody>
                <tr>
                    <td style="width:24% !important;font-weight:normal;">
                        """+patnaHotLine()+"""
                    </td>
                    <td style="width:1% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:auto !important;font-weight:normal;">
                        <div style="display:inline-block;width:100%font-weight:normal;">
                            <b style="float:left;margin-right:15px; font-weight: normal;">"""+normalEmergencyLabel()+"""</b>

                        </div>
                    </td>
                    <td style="width:1% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:19% !important; text-align:right;font-weight:normal;">
                        <div style="display:inline;width:100%font-weight:normal;">
                            <b style="float:left; font-weight: normal;">"""+normalEmailLabel()+"""</b>
                        </div>

                    </td>
                    <td style="width:1% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:auto !important;">
                        <div style="">
                            <b style="font-weight: normal;" class="red">"""+websitLabel()+"""</b>
                        </div>
                    </td>
                    <td style="width:1% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:23%!important;">
                        <div class="text-right">
                            <b style="font-weight: normal;">
                                <span style="float:right;" ng-if="(performingLocationId == ('reportingEngine.footerPage.patnaCode' | translate))"
                                       data-ng-bind="&quot;reportingEngine.footerPage.patna.cin&quot; | translate"
                                      class="ng-binding">CIN: U74999DL2015PTC283932</span></b>
                        </div>
                    </td>
                </tr>
                </tbody>
            </table>
        </div>
    </div>
</div></body></html>
"""
		}else{
			log.info("====================FACILITY IN OPDDUMP is gurgaon ="+row.FACILITY_ID)
			footer = """<html><head><link href=\""""+cssPath+"""css/reportHeadFootHhr.css"  rel="stylesheet" /><script>
        function subst() {
            var vars={};
            var x=document.location.search.substring(1).split('&');
            for (var i in x) {var z=x[i].split('=',2);vars[z[0]] = unescape(z[1]);}
            var x=['frompage','topage','page','webpage','section','subsection','subsubsection'];
            for (var i in x) {
                var y = document.getElementsByClassName(x[i]);
                for (var j=0; j<y.length; ++j) y[j].textContent = vars[x[i]];
            }
        }
        </script>
</head>
<body onload="subst()"><div class="pdfFooter">
<div class="text-right"><b>Page No. </b> <span class="page"></span> of <span class="topage"></span></div>"""+authFoother+
					"""<div class="line2" style="font-weight: normal;">
        <div>
            <table style="line-height:20px;font-size:10px;font-weight:normal;">
                <tbody>
                <tr>
                    <td style="width:67% !important;">
                        <div style="display:inline;width:100%;font-weight:normal;">
                            <div style="font-weight:normal;"><span style="font-weight:normal;"
                                    ng-if="(performingLocationId == ('reportingEngine.footerPage.gurugramCode' | translate))"
                                    data-ng-bind="&quot;reportingEngine.footerPage.gurugram.addressLabel&quot; | translate"
                                    class="ng-binding">Regd. Office: Global Health Limited, E-18 Defence colony , New Delhi, 110024, India</span></div>
                        </div>
                    </td>
                    <td style="width:33% !important;font-weight:normal;">
                        <div style="display:inline;width:100%;font-weight:normal;">
                            """+hhrTelePhoneAndFaxLabel()+"""
                        </div>
                    </td>
                </tr>
                </tbody>
            </table>
        </div>
<div>
            <table style="line-height:20px;font-size:10px;font-weight:normal; width: 100%">
                <tbody>
                <tr>
                    <td style="width:24% !important;font-weight:normal;">
                        <span style="font-weight:normal;" ng-if="performingLocationId == ('reportingEngine.footerPage.gurugramCode' | translate)"
                              data-ng-bind="&quot;reportingEngine.footerPage.gurugram.hotLineLabel&quot; | translate"
                              class="ng-binding">24X7 hot-line: +91(124)4141414</span>
                    </td>
                    <td style="width:1% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:auto !important;font-weight:normal;">
                        <div style="display:inline-block;width:100%font-weight:normal;">
                            <b style="float:left;margin-right:15px; font-weight: normal;">"""+normalEmergencyLabel()+"""</b>

                        </div>
                    </td>
                    <td style="width:1% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:19% !important; text-align:right;font-weight:normal;">
                        <div style="display:inline;width:100%font-weight:normal;">
                            <b style="float:left; font-weight: normal;">"""+normalEmailLabel()+"""</b>
                        </div>

                    </td>
                    <td style="width:1% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:auto !important;">
                        <div style="">
                            <b style="font-weight: normal;" class="red">"""+websitLabel()+"""</b>
                        </div>
                    </td>
                    <td style="width:1% !important; vertical-align:bottom"><span
                            style="height:16px; width:1px; background:#ccc; display:inline-block;"></span></td>
                    <td style="width:23%!important;">
                        <div class="text-right">
                            <b style="font-weight: normal;">
                                <span style="float:right;" ng-if="(performingLocationId == ('reportingEngine.footerPage.gurugramCode' | translate))"
                                       data-ng-bind="&quot;reportingEngine.footerPage.gurugram.cin&quot; | translate"
                                      class="ng-binding">CIN: U85110DL2004PLC128319</span></b>
                        </div>
                    </td>
                </tr>
                </tbody>
            </table>
        </div>
    </div>
</div></body></html>
"""
		}
		return footer;
	}
    public String radReportSqlQuery(){
        String Query = """select req.ORDER_ID OrderID,
        	pe.ATTEND_PRACTITIONER_ID attendDoctorId,
        	attend.PRACTITIONER_NAME attendDoctorName,
            req.physician_id RefDoctorId,
            refdoc.practitioner_name ReferralDoctorName,
            refdoc.primary_speciality_code ReferralDoctorSpecialityID,
            refSpec.long_desc ReferralDoctorSpecialityDesc,
            req.patient_id RegistrationNo,
            req.patient_class InPatientOrOut,
            nvl(p.contact1_no, p.contact2_no) PatientPhoneNo,
            p.EMAIL_ID PatientEmailAddress,
            p.name_prefix PatientNamePrefix,
            p.first_name FIRSTNAME,
            p.second_name middleName,
            p.family_name LastName,
            p.sex PatientGender,
            TO_CHAR(p.date_of_birth, 'YYYYMMDDHH24MISS') DateofBirth,
            req.added_facility_id IACODE,
            req.encounter_id EncounterId,
            TO_CHAR(rep.modified_date, 'YYYYMMDDHH24MISS') ModifiedDate,
            TO_CHAR(pe.VISIT_ADM_DATE_TIME, 'YYYYMMDDHH24MISS') VISITDATE,
            TO_CHAR(pe.DISCHARGE_DATE_TIME, 'YYYYMMDDHH24MISS') dischargeDate,
            'RD' OrderType,
            TO_CHAR(req.request_date, 'YYYYMMDDHH24MISS') OrderDatetime,
            '' SpecimenType,
            '' SpecimenCollectDate,
            '' SpecimenDispatchDate,
            '' SpecimenRcptDate,
            '' SpecimenRegDate,
            TO_CHAR(rep.reported_date, 'YYYYMMDDHH24MISS') VerifiedDateTime,
            TO_CHAR(rep.reported_date, 'YYYYMMDDHH24MISS') ResultDateTime,
            rep.radiologist_id ReleasedByDoctorId,
            radiologist.practitioner_name ReleasedByDoctorName,
            NVL(to_char(rep.ext_appl_accession_num), req.ORDER_ID) OrderNo,
            rep.exam_code TestID,
            rep.exam_code Parameterid,
            rdesc.LONG_DESC TestName,
            rdesc.SECTION_CODE MODALITY,
            '' GroupSeqNo,
            '' SeqNo,
            '' ResultPrefix,
            '' ResultORsusceptibility,
            '' UnitidORantibioticid,
            '' UnitORantibioticname,
            '' ReferenceRangeLow,
            '' ReferenceRangeHigh,
            '' ReferenceRange,
            '' ReviewedDate,
            rep.radiologist_id LabDoctorID,
            radiologist.practitioner_name LabDoctorName,
            radiologist.practitioner_sign verifieddocsign,
            radiologist.position_code verifieddocpositioncode,
            to_char(radiologist.modified_date, 'YYYYMMDDHH24MISS') verifieddocmodifieddate,
            radDesignation.position_desc LabDoctorDesignation,
            rep.added_facility_id LabLocationId,
            'Gurgaon' LabLocationName,
            '' normal,
            '' HideTest,
            '' HideComments,
            rep.ADDED_FACILITY_ID HsplocationId,
            rep.ext_appl_accession_num AccessionNumber,
            rep.status TestStatus,
            '' TestStatus, 'Authorized' TestStatusName,
            'Gurgaon' LocationName,
            'NO' IsOrganism,
            replace(DBMS_LOB.substr(rep.report_text,200000), '"', '""') Result2,
            rep.report_text,
            '' LabNumber,
            '' VIP,
            '' AdmitDoctorID,
            '' AdmitDoctorName,
            '' ResultTextCode,
            '' AntibioticGroupCode,
            '' OrganismCode,
            '' OrganismName,
             TO_CHAR(rep.reported_date, 'YYYYMMDDHH24MISS') ReleaseDate,
            NVL(clinic.long_desc, ip_clinic.long_desc) OrderingLocation,
            altReleaseDoc.appl_user_name AltReleasedByDoctorName,
            altLabDoc.appl_user_name AltLabDoctorName,
            '' AltLabDoctorDesignation,
            'Rad' TestType,
            '' OrganismComment
            from rd_request req, MPHRX_MPPT_VW p, rd_exam_view_requested rep, am_practitioner refdoc, am_speciality refSpec, am_practitioner radiologist,am_practitioner attend, am_position radDesignation,
            op_clinic clinic, ip_nursing_unit ip_clinic, sm_appl_user altLabDoc, sm_appl_user altReleaseDoc, RD_EXAMS rdesc, PR_Encounter pe
            where altReleaseDoc.appl_user_id(+) = rep.radiologist_id
            and altLabDoc.appl_user_id(+) = rep.radiologist_id
            and ip_clinic.facility_id(+) = req.added_facility_id
            and ip_clinic.nursing_unit_code(+) = req.ref_source_code
            and clinic.facility_id(+) = req.added_facility_id
            and clinic.clinic_code(+) = req.ref_source_code
            and p.patient_id = req.patient_id
            and req.ORDER_ID = rep.ORDER_ID
            and req.operating_facility_id = rep.operating_facility_id
            and radiologist.practitioner_id(+) = rep.radiologist_id
            and radDesignation.position_code(+) = radiologist.position_code
            and refdoc.practitioner_id(+) = req.physician_id
            and refSpec.speciality_code(+) = refdoc.primary_speciality_code
			and attend.practitioner_id(+) = pe.ATTEND_PRACTITIONER_ID
            and report_text is not null 
	        and rep.status in ('70','85','99')
            and rdesc.exam_code(+) = rep.exam_code
            and pe.ENCOUNTER_ID(+) = req.ENCOUNTER_ID
	        --and pe.FACILITY_ID = req.OPERATING_FACILITY_ID
	        and pe.FACILITY_ID = req.ORDERING_FACILITY_ID
	        -- and req.patient_id in ('MB00000287','MB00001363','MM00947523')
            """;
        return Query

    }

    public def radQuery(String QueryDate,String serviceNameOrg,Map driverMap,boolean isReconJob = false)
    {
        int counter = 0
        Boolean isSuccess = false
        String collectionName ="radReportDump"
        serviceName = serviceNameOrg;
        try {
            def instance = driverMap.instance
            def user = driverMap.user
            def password = driverMap.password
            def driver = driverMap.driver

            def sql = Sql.newInstance("${instance}", "${user}", "${password}", "${driver}")
            String radReportQuery = radReportSqlQuery()
            String Query = """  ${radReportQuery} 
                                ${QueryDate} 
                            """
            println "running Query " + Query
            if (sql) {
                log.error("[Message radQuery: ${serviceName}]: Database Connected for Query [${Query}]")
            }
            //String collectionName = "radReportDump"
            sql.eachRow(Query)
                    {
                        row ->
                            Map docMap = [:]
                            // variables for store data in mongo
                            //Note-:  Hare ReleasedByDoctorName, LabDoctorName,AltReleasedByDoctorName, and AltLabDoctorName  All are same
                            // so we are putting only LabDoctorName full details in mongo
                            String patId = row.REGISTRATIONNO
                            if(patId.find(/(?i)^\s*(GG)\.*/)){
                                println ("Patient record found with  patientId starting with GG: "+patId+".Skipping this record !!\n\n");
                                return
                            }
							println("Inside radQuery function :: TestStatus : "+row.TESTSTATUS+", patentId : "+row.REGISTRATIONNO+"\n\n")
							if(row.TESTSTATUS == '99'){
								if(isORUExists(row)){
									println("ORU exists in hl7Message collection for patientId : "+row.REGISTRATIONNO+", accessionNo : "+row.ACCESSIONNUMBER+", modifiedDate : "+row.MODIFIEDDATE+"\n\n")
								}
								else{
									println("ORU not exists in hl7Message collection for patientId : "+row.REGISTRATIONNO+", accessionNo : "+row.ACCESSIONNUMBER+", modifiedDate : "+row.MODIFIEDDATE+" || Skipping this record !!\n\n")
									return
								}
							}

                            println ("Patient record found with  patientId starting with MM: "+patId+"\n\n");


                            boolean isReconSuccess = false
                            if(isReconJob == true){
                                isReconSuccess = radiologyReconInsert(row)
                                if(!isReconSuccess){
                                    return
                                }
                            }
                            def OrderID = ""
                            def AttendDoctorId = ""
                            def AttendDoctorFirstName = ""
                            def AttendDoctorLastName = ""
                            def AttendDoctorMiddleName = ""
                            def AttendDoctorPrefixName = ""
                            def RefDoctorId = ""
                            def ReferralDoctorFirstName = ""
                            def ReferralDoctorLastName = ""
                            def ReferralDoctorMiddleName = ""
                            def ReferralDoctorPrefixName = ""
                            def ReferralDoctorSpecialityID = ""
                            def ReferralDoctorSpecialityDesc = ""
                            def patientId = ""
                            def InPatientOrOut = ""
                            def PatientPhoneNo = ""
                            def PatientEmailAddress =""
                            def PatientNamePrefix = ""
                            def FirstName = ""
                            def LastName = ""
                            def MiddleName = ""
                            def PatientGender = ""
                            def DateofBirth = ""
                            def IACODE = ""
                            def EncounterId = ""
                            def encounterStartDate = ""
                            def encounterEndDate = ""
                            def OrderType = ""
                            def OrderDatetime = ""
                            def VerifiedDateTime = ""
                            def ResultDateTime = ""
                            def OrderNo = ""
                            def TestID = ""
                            def Parameterid = ""
                            def LabDoctorID = ""
                            def LabDoctorFirstName = ""
                            def LabDoctorLastName = ""
                            def LabDoctorMiddleName = ""
                            def LabDoctorPrefixName = ""
                            def LabDoctorDesignation = ""
                            def LabLocationId = ""
                            def LabLocationName = ""
                            def HsplocationId = ""
                            def AccessionNumber = ""
                            def TestStatusName = ""
                            def LocationName = ""
                            def IsOrganism = ""
                            def Result2 = ""  // Not in use
                            def report_text = "" // Same as Result2 above
                            def ReleaseDate = ""
                            def OrderingLocation = ""
                            def AltReleasedByDoctorName = ""
                            def AltLabDoctorName = ""
                            def TestType = ""
                            def TestName = ""
                            def Modality =""
                            def modifiedDate = ""
							def TestStatus = ""
							def labDoctorSignPath = ""
							def labDoctorPositionCode = ""
							def labDoctorLastModifiedDate = ""

                            if (row.ORDERID)
                                OrderID = row.ORDERID


                            if (row.ATTENDDOCTORID)
                            AttendDoctorId = row.ATTENDDOCTORID

                            if (row.ATTENDDOCTORNAME)
                            {
                            (AttendDoctorPrefixName,AttendDoctorFirstName,AttendDoctorMiddleName,AttendDoctorLastName) = getNameFromString(row.ATTENDDOCTORNAME)
                            }

                            if (row.REFDOCTORID)
                                RefDoctorId = row.REFDOCTORID
                            if (row.REFERRALDOCTORNAME)
                            {
                                (ReferralDoctorPrefixName,ReferralDoctorFirstName,ReferralDoctorMiddleName,ReferralDoctorLastName) = getNameFromString(row.REFERRALDOCTORNAME)
                            }
                            if(row.REFERRALDOCTORSPECIALITYDESC)
                            {
                                ReferralDoctorSpecialityDesc = row.REFERRALDOCTORSPECIALITYDESC
                            }
                            if(row.REFERRALDOCTORSPECIALITYID)
                            {
                                ReferralDoctorSpecialityID = row.REFERRALDOCTORSPECIALITYID
                            }
                            if (row.REGISTRATIONNO)
                                patientId = row.REGISTRATIONNO
                            if (row.INPATIENTOROUT)
                                InPatientOrOut = row.INPATIENTOROUT
                            if (row.PATIENTPHONENO)
                                PatientPhoneNo = row.PATIENTPHONENO
                            if (row.PATIENTNAMEPREFIX)
                                PatientNamePrefix = row.PATIENTNAMEPREFIX
                            if (row.FIRSTNAME)
                                FirstName = row.FIRSTNAME
                            if (row.LASTNAME)
                                LastName = row.LASTNAME
                            if(row.MIDDLENAME)
                                MiddleName = row.MIDDLENAME
                            if (row.PATIENTGENDER)
                                PatientGender = row.PATIENTGENDER
                            if (row.DATEOFBIRTH)
                                DateofBirth = row.DATEOFBIRTH
                            if (row.IACODE)
                                IACODE = row.IACODE
                            if (row.ENCOUNTERID)
                                EncounterId = row.ENCOUNTERID
                            if (row.ORDERTYPE)
                                OrderType = row.ORDERTYPE
                            if (row.ORDERDATETIME)
                                OrderDatetime = row.ORDERDATETIME
                            if (row.VERIFIEDDATETIME)
                                VerifiedDateTime = row.VERIFIEDDATETIME
                            if (row.RESULTDATETIME)
                                ResultDateTime = row.RESULTDATETIME
                            if (row.ORDERNO)
                                OrderNo = row.ORDERNO
                            if (row.TESTID)
                                TestID = row.TESTID
                            if (row.PARAMETERID)
                                Parameterid = row.PARAMETERID
                            if (row.LABDOCTORID)
                                LabDoctorID = row.LABDOCTORID
                            if (row.LABDOCTORNAME)
                            {
                                (LabDoctorPrefixName,LabDoctorFirstName,LabDoctorMiddleName,LabDoctorLastName) =getNameFromString(row.LABDOCTORNAME)
                            }
							if (row.verifieddocsign)
								labDoctorSignPath = saveSignature(row, "verifieddoc")
                            if (row.LABDOCTORDESIGNATION)
                                LabDoctorDesignation = row.LABDOCTORDESIGNATION
                            if (row.LABLOCATIONID)
                                LabLocationId = row.LABLOCATIONID
                            if (row.LABLOCATIONNAME)
                                LabLocationName = row.LABLOCATIONNAME
                            if (row.HSPLOCATIONID)
                                HsplocationId = row.HSPLOCATIONID
                            if (row.ACCESSIONNUMBER)
                                AccessionNumber = row.ACCESSIONNUMBER
                            if (row.TESTSTATUSNAME)
                                TestStatusName = row.TESTSTATUSNAME
                            if (row.LOCATIONNAME)
                                LocationName = row.LOCATIONNAME
                            if (row.ISORGANISM)
                                IsOrganism = row.ISORGANISM
                            if (row.RESULT2)
                                Result2 = row.RESULT2
                            if(row.REPORT_TEXT)
                            {
                                def report_textStr = row.REPORT_TEXT.getSubString(1, (int)row.REPORT_TEXT.length())
                                report_text = report_textStr.toString()
                            }

                            if (row.RELEASEDATE)
                                ReleaseDate = row.RELEASEDATE
                            if (row.ORDERINGLOCATION)
                                OrderingLocation = row.ORDERINGLOCATION
                            if (row.ALTRELEASEDBYDOCTORNAME)
                                AltReleasedByDoctorName = row.ALTRELEASEDBYDOCTORNAME
                            if (row.ALTLABDOCTORNAME)
                                AltLabDoctorName = row.ALTLABDOCTORNAME
                            if (row.TESTTYPE)
                                TestType = row.TESTTYPE
                            if(row.PATIENTEMAILADDRESS)
                                PatientEmailAddress = row.PATIENTEMAILADDRESS
                            if(row.TESTNAME)
                                TestName = row.TESTNAME
                            if(row.MODALITY)
                                Modality = row.MODALITY
                            if(row.VISITDATE)
                                encounterStartDate = row.VISITDATE
                            if(row.DISCHARGEDATE)
                                encounterEndDate = row.DISCHARGEDATE
                            if(row.MODIFIEDDATE)
                                modifiedDate = row.MODIFIEDDATE
							if(row.TESTSTATUS)
								TestStatus = row.TESTSTATUS
							if(row.verifieddocpositioncode)
								labDoctorPositionCode = row.verifieddocpositioncode
							if(row.verifieddocmodifieddate)
								labDoctorLastModifiedDate = row.verifieddocmodifieddate

                            docMap.put("patientId", patientId)
                            docMap.put("OrderID", OrderID)
                            docMap.put("AttendDoctorId", AttendDoctorId)
                            docMap.put("AttendDoctorFirstName", AttendDoctorFirstName)
                            docMap.put("AttendDoctorLastName", AttendDoctorLastName)
                            docMap.put("AttendDoctorMiddleName", AttendDoctorMiddleName)
                            docMap.put("AttendDoctorPrefixName", AttendDoctorPrefixName)
                            docMap.put("status", "PENDING")
                            docMap.put("retries", 0)
                            docMap.put("RefDoctorId", RefDoctorId)
                            docMap.put("ReferralDoctorFirstName", ReferralDoctorFirstName)
                            docMap.put("ReferralDoctorLastName", ReferralDoctorLastName)
                            docMap.put("ReferralDoctorMiddleName", ReferralDoctorMiddleName)
                            docMap.put("ReferralDoctorPrefixName", ReferralDoctorPrefixName)
                            docMap.put("ReferralDoctorSpecialityID", ReferralDoctorSpecialityID)
                            docMap.put("ReferralDoctorSpecialityDesc", ReferralDoctorSpecialityDesc)
                            docMap.put("InPatientOrOut", InPatientOrOut)
                            docMap.put("PatientPhoneNo", PatientPhoneNo)
                            docMap.put("PatientNamePrefix", PatientNamePrefix)
                            docMap.put("FirstName", FirstName)
                            docMap.put("MiddleName", MiddleName)
                            docMap.put("LastName", LastName)
                            docMap.put("PatientGender", PatientGender)
                            docMap.put("DateofBirth", DateofBirth)
                            docMap.put("IACODE", IACODE)
                            docMap.put("EncounterId", "${EncounterId}")
                            docMap.put("encounterStartDate", encounterStartDate)
                            docMap.put("encounterEndDate", encounterEndDate)
                            docMap.put("OrderType", OrderType)
                            docMap.put("OrderDatetime", OrderDatetime)
                            docMap.put("VerifiedDateTime", VerifiedDateTime)
                            docMap.put("ResultDateTime", ResultDateTime)
                            //docMap.put("ResultDateTime", ResultDateTime)
                            docMap.put("OrderNo", OrderNo)
                            docMap.put("TestID", TestID)
                            docMap.put("Parameterid", Parameterid)
                            docMap.put("LabDoctorID", LabDoctorID)
                            docMap.put("LabDoctorFirstName", LabDoctorFirstName)
                            docMap.put("LabDoctorLastName", LabDoctorLastName)
                            docMap.put("LabDoctorMiddleName", LabDoctorMiddleName)
                            docMap.put("LabDoctorPrefixName", LabDoctorPrefixName)
                            docMap.put("LabDoctorDesignation", LabDoctorDesignation)
							docMap.put("LabDoctorSignPath",labDoctorSignPath)
							docMap.put("labDoctorPositionCode",labDoctorPositionCode)
							docMap.put("labDoctorLastModifiedDate",labDoctorLastModifiedDate)
							docMap.put("LabLocationId", LabLocationId)
                            docMap.put("LabLocationName", LabLocationName)
                            docMap.put("HsplocationId", HsplocationId)
                            docMap.put("AccessionNumber", "${AccessionNumber}")
                            docMap.put("TestStatusName", TestStatusName)
                            docMap.put("LocationName", LocationName)
                            docMap.put("IsOrganism", IsOrganism)
                            // docMap.put("Result2", Result2)
                            docMap.put("ReportText", report_text)
                            docMap.put("ReleaseDate", ReleaseDate)
                            docMap.put("OrderingLocation", OrderingLocation)
                            docMap.put("AltReleasedByDoctorName", AltReleasedByDoctorName)
                            docMap.put("AltLabDoctorName", AltLabDoctorName)
                            docMap.put("TestType", TestType)
                            docMap.put("PatientEmailAddress",PatientEmailAddress)
                            docMap.put("TestName",TestName)
                            docMap.put("Modality",Modality)
                            docMap.put("ModifiedDate",modifiedDate)
							docMap.put("testStatus",TestStatus)

                            isSuccess = false
                            isSuccess = save(docMap, collectionName)
                            if (isSuccess) {
                                counter++
                                log.info "[Message: ${serviceName}]: Inserted document entry pkID[${patientId}] in ${collectionName} with status [PENDING]"
                            }else{
                                Log.error "[Message: ${serviceName}]: ERROR: Failed to insert document entry pkID[${patientId}] in ${collectionName} with status [PENDING]"
                            }
                    }
            sql.close()
            return counter
        }
        catch (Exception ex) {
            log.error("Error: Exception occurred to process ${collectionName} ${serviceName} custom job ", ex)
            throw ex
        }
    }
    public boolean radiologyReconInsert(def rowMap){
        //Fetching the data from radReportDump
        boolean isSuccess = false
        String patientId = rowMap.REGISTRATIONNO
        String AccessionNumber =  rowMap.ACCESSIONNUMBER
        String modifiedDate = rowMap.MODIFIEDDATE

        if((patientId && patientId != '') && (AccessionNumber && AccessionNumber != '') && (modifiedDate && modifiedDate != '')) {
            String searchMap = ""
            String collectionName = "radReportDump"
            String sortMap = ""
            int pageSize = 1
            int offset = 0
            Map searchResult = [:]
            searchMap = '{"$and":[{"patientId" : "'+patientId+'"},{"AccessionNumber" : "'+AccessionNumber+'"},{"status":{"$ne":"FAILED"}}]}'
            sortMap = '{_id : -1}'
            searchResult = search(searchMap, collectionName, sortMap, pageSize, offset)
            if (searchResult.matchCount == 0) {
                log.info("[Message: ${serviceName}]: No instance of patientID [${patientId}] with accessionNumber [${AccessionNumber}] exists in [${collectionName}]. Going to insert record")
                isSuccess = true
            } else {
                searchResult.objects.each {
                    long _id = it._id
                    String oldmodifiedDate = it.ModifiedDate
                    if (oldmodifiedDate && oldmodifiedDate != '') {
                        long oldModified = oldmodifiedDate.toLong()
                        long newModified = modifiedDate.toLong()
                        if (newModified > oldModified) {
                            log.info("[Message: ${serviceName}]: Going to insert instance with patientID [${patientId}] with accessionNumber [${AccessionNumber}] as new ModifiedDate ${modifiedDate} is greater than old ModifiedDate [${oldmodifiedDate}]")
                            isSuccess = true
                        } else {
                            log.info("[Message: ${serviceName}]: Skipping to insert instance with patientID [${patientId}] with accessionNumber [${AccessionNumber}] as new ModifiedDate ${modifiedDate} is not greater than old ModifiedDate [${oldmodifiedDate}] ")
                            isSuccess = false
                        }
                    }
                }
            }
        }
        else
        {
            log.info("[Message: ${serviceName}]: Skipping to insert instance because either  patientID [${patientId}] or accessionNumber [${AccessionNumber}] or ModifiedDate ${modifiedDate} is empty")
            isSuccess = false
        }
        return isSuccess
    }

	def isORUExists(def row){
		int pageSize = 1
		int offset = 0
		String patientId = row.REGISTRATIONNO
		String accession = row.ACCESSIONNUMBER
		String searchMap = '{"$and": [{"processed": {"$in": ["PROCESSED","PENDING","PROCESSING","FAILED"]}},{"patientId":"'+patientId+'"},{"accession":"'+accession+'"},{"messageType":"ORU"},{"reportStatus":"F"}]}'
		String sortMap = '{_id : -1}'
		Map searchResult = search(searchMap, "hl7Message", sortMap, pageSize, offset)
		if (searchResult.matchCount > 0) {
			return true
		}
		return false
	}
    public def getNameFromString(String name)
    {
        String FirstName =""
        String MiddleName = ""
        String LastName =""
        String NamePrefix =""
        Boolean namePreFlag= false
        if(name.find(/(?i)^\s*(Miss|Dr|Mr|Mrs)\.*\s+/))
        {
            namePreFlag = true
        }

        if(name && name != null)
        {
            List physNameList = name.split(/\s+/)
            if(namePreFlag)
            {
                NamePrefix = physNameList.remove(0)
            }

            if(physNameList.size() >= 4)
            {
                FirstName = physNameList.remove(0)
                MiddleName = physNameList.remove(0)
                LastName = physNameList.join(" ")
            }
            else if(physNameList.size() == 3)
            {
                FirstName = physNameList.remove(0)
                MiddleName = physNameList.remove(0)
                LastName = physNameList.remove(0)
            }
            else if(physNameList.size() == 2)
            {
                FirstName = physNameList.remove(0)
                LastName = physNameList.remove(0)
            }
            else if(physNameList.size() == 1)
            {
                FirstName = physNameList.remove(0)
            }
        }
        return [NamePrefix,FirstName,MiddleName,LastName]
    }

    public def GetJobLastRunTime(String serviceName)
    {
        String searchMap = ""
        String LastRunDate = ""
        String collectionName = "customJobLastRunDetails"
        int pageSize = 1
        int offset = 0
        Map searchResult = [:]
        String sortMap = ""
        def ClusterID =""
        searchMap = '{"JobName" : "' + serviceName + '"}'
        sortMap = '{_id : -1}'
        searchResult = search(searchMap, collectionName, sortMap, pageSize, offset)
        if (searchResult.matchCount > 0) {
            searchResult.objects.each {
                LastRunDate = it.JobStartTime
            }
            if(LastRunDate)
                LastRunDate = getDateString(LastRunDate)
        }
        else
        {
            log.error("[Message: ${serviceName}]: Error:  custom job [${serviceName}] Not Found >>>>>>")

        }
        return LastRunDate
    }

    public String getDateString(String dateStr,String SendDateFormate = "",String ReceiveDateFormat = "")
    {

        String dateString
        try {
            SimpleDateFormat formatter
            SimpleDateFormat formatter1
            if(SendDateFormate)
                formatter = new SimpleDateFormat(SendDateFormate)
            else
                formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")

            Date date = formatter.parse(dateStr)

            if(ReceiveDateFormat)
                formatter1 = new SimpleDateFormat(ReceiveDateFormat)
            else
                formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

            dateString = formatter1.format(date)

            //def dateStr = '2016-04-25T10:40:50.315Z'
            // SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
        } catch (Exception ex) {
            log.error("[Message: ${serviceName}]:Invalid date format",ex)
            throw ex
        }
        return dateString
    }

    public Boolean updateCustomJobLog(Map msgCountMap,String serviceName,def startTime)
    {
        String searchMap = ""
        boolean isSuccess = false
        String ServiceNameLog = serviceName
        ServiceNameLog = ServiceNameLog.replaceFirst(/Service$/,"")
        String collectionName = "jobConfiguration"
        int pageSize = 1
        int offset = 0
        Map searchResult = [:]
        String sortMap = ""
        def ClusterID =""
        searchMap = '{"serviceName" : "' + ServiceNameLog + '","enabled":true}'
        sortMap = '{_id : 1}'
        searchResult = search(searchMap, collectionName, sortMap, pageSize, offset)
        if (searchResult.matchCount > 0) {
            if(searchResult.matchCount > 1)
                log.error("[Message: ${serviceName}]: Error:  custom job [${ServiceNameLog}] More then One job exist for the same name >>>>>>")
            searchResult.objects.each {
                ClusterID = it.clusterId
            }
            if(!ClusterID)
                log.error("[Message: ${serviceName}]: Error:  custom job [${ServiceNameLog}] Cluster ID not Present  >>>>>>")

            msgCountMap.put("JobEndTime",new Date())
            msgCountMap.put("JobStartTime",startTime)
            msgCountMap.put("JobName",serviceName)
            msgCountMap.put("JobClusterID",ClusterID)
            isSuccess = save (msgCountMap, "customJobLastRunDetails")
            if (isSuccess) {
                log.info "[Message: ${serviceName}]: Inserted date (${msgCountMap}) in customJobLastRunDetails collection==="
            }
        }
        else
        {
            log.error("[Message: ${serviceName}]: Error:  custom job [${ServiceNameLog}] Not Found >>>>>>")

        }
        return isSuccess
    }

    public void afterPropertiesSet() {
        mongoClient = client()
        db = getDB(mongoClient)
    }

    public void destroy() {
        mongoClient?.close()
    }

    public def MongoClient client() {
        String host = grailsApplication.config.grails.mongo.host
        String port = grailsApplication.config.grails.mongo.port
        String username = grailsApplication.config.grails.mongo.username ?: null
        String password = grailsApplication.config.grails.mongo.password ?: null
        String databaseName = grailsApplication.config.grails.mongo.databaseName
        Integer connectionsPerHost = grailsApplication.config.grails.mongo.connectionsPerHost ? grailsApplication.config.grails.mongo.connectionsPerHost.toInteger() : 10


        if (mongoClient == null) {
            MongoClientOptions options = MongoClientOptions.builder()
                    .connectionsPerHost(connectionsPerHost)
                    .build();

            ServerAddress sa = new ServerAddress(host, port.toInteger())

            if (username && password) {
                List<MongoCredential> mongoCredentialList = [];

                mongoCredentialList.add(MongoCredential.createMongoCRCredential(username, databaseName, password.toCharArray()))
                return new MongoClient(sa, mongoCredentialList, options)
            } else {
                return new MongoClient(sa, options)
            }
        } else {
            return mongoClient
        }
    }

    public DB getDB(MongoClient client) {
        String databaseName = grailsApplication.config.grails.mongo.databaseName
        return client.getDB(databaseName);
    }

    public DBCollection collection(collectionName) {
        return db.getCollection(collectionName)
    }

    public def save(def mapData, String collectionName) {
        boolean isSuccess = false;
        boolean isinsertISO = false;
	if(mapData.containsKey('insertISODate')){
		isinsertISO = true;
	}
        DBCollection collection = collection(collectionName);
        // def JsonData = toJSON(mapData).toString()
        def JsonData = JsonOutput.toJson(mapData).toString()
        DBObject object = (DBObject) JSON.parse(JsonData)

        while (1) {
            try {
                object.put("_id", getNextSequence(collectionName))
                object.put("GetDataTime", new Date())
		if(isinsertISO == true){
                	object.put("insertISODate", new Date())
		}
                WriteResult result = collection.insert(object, WriteConcern.SAFE)
                isSuccess = true
                break;
            } catch (MongoException ex) {
                isSuccess = false
                log.error("Exception in saving object ${JsonData.toString()}", ex)

                try {
                    if (JSON.parse(ex.message).code == 11000) {
                        log.error("Duplicate key problem so need to try again....")
                        continue;
                    } else {
                        break;
                    }

                } catch (Exception e) {
                    log.error("Exception in parsing json ${ex.message}")
                    break;
                }
            }
        }

        return isSuccess
    }

    public Object getNextSequence(String name) throws Exception {
        DBCollection collection = collection("counters")
        BasicDBObject find = new BasicDBObject();
        find.put("_id", name);
        BasicDBObject update = new BasicDBObject();
        update.put('$inc', new BasicDBObject("seq", 1));
        DBObject obj = collection.findAndModify(find, null, null, false, update, true, true);

        return obj.get("seq");
    }

    public def search(def searchCriteria, String collectionName, def sortCriteria, int pageSize = 10, int offset = 0) {

        Map searchResult = [:]
        DBObject object = null


        DBCollection collection = collection(collectionName);
        DBObject searchCriteriaObj = (DBObject) JSON.parse(searchCriteria.toString())
        DBObject sortCriteriaObj = (DBObject) JSON.parse(sortCriteria.toString())

        println "searchCriteriaObj ... ${searchCriteriaObj.toString()} "

        List<DBObject> objects = []
        def cursor = null
        int matchCount = 0
        try {
            cursor = collection.find(searchCriteriaObj).skip(offset)

            if (sortCriteria) {
                cursor = cursor.sort(sortCriteriaObj)
            }

            cursor = cursor.limit(pageSize)
            Iterator<DBObject> iterator = cursor.iterator()

            while (iterator.hasNext()) {
                objects.add(iterator.next())
                matchCount++
            }

            searchResult.objects = objects
            searchResult.matchCount = matchCount
            searchResult.totalCount = count(searchCriteria, collectionName)


        } catch (MongoException ex) {
            log.error("Exception in search with criteria ${searchCriteria.toString()}", ex)
        } finally {
            cursor?.close()
        }

        return searchResult
    }
    public def count(def searchCriteria, String collectionName) {
        DBCollection collection = collection(collectionName);
        DBObject searchCriteriaObj = (DBObject) JSON.parse(searchCriteria.toString())
        Long count = 0;
        Map responseMap = [:]

        try {
            responseMap.count = collection.count(searchCriteriaObj)
        } catch (MongoException ex) {
            log.error("Exception in count with criteria ${searchCriteria}", ex)
        }

        return responseMap
    }

	public Map extractDataFromDcsXmlFile(String xmlString){
		log.info("Inside extractDataFromDcsXmlFile");
		NodeChild xmlObj = new XmlSlurper().parseText(xmlString);
		Map<String, String> xmlDataMap = new HashMap();

		Map codeMap = configObject.discridCodeMap.get("COMMON");
		codeMap.putAll(configObject.discridCodeMap.get(configObject.environmentType));

		for(code in codeMap){
			log.info("Working for Code: ${code}")
			String dataType = configObject.discridDataTypes.get(code.key);
			NodeChild node = getNodeFromXml(xmlObj, dataType, code.value);
			String parsedData = "parseDataFrom_${dataType.replace("-","_")}"(node, code.key);
			log.info("Parsed data for Code: ${code.value} is ${parsedData}");
			xmlDataMap.put((code.key), parsedData );
			if(!parsedData || parsedData.trim() == "")
				xmlDataMap.put(("${code.key}_condition"), "none");
			else
				xmlDataMap.put(("${code.key}_condition"), "block");
		}
		return xmlDataMap;
	}

	public NodeChild getNodeFromXml(NodeChild xmlObj, String tagName, String discrid){
		log.info("getNodeFromXml : Going to fetch node from xmlObj for tagName: ${tagName} and discrid: ${discrid}");
		NodeChild node =  xmlObj.'**'.find { node -> node.name() == tagName && node.@DISCRID == discrid };

		log.info("getNodeFromXml : Fetched node: ${node?.name()}: ${node}");
		if(!node)
			log.info("getNodeFromXml : Didn't fetched any node: ${node}");

		return node
	}

	public String parseDataFrom_PARAGRAPH(NodeChild node, String key){
		log.info("Inside parseDataFrom_PARAGRAPH for Id : ${key}");
		String data = "";
		if(node){
			ArrayList paragraph = node['LIST-BOX-WTD'].DATA.findAll{n -> n.@SELECTED == "true"}*.text();
			log.info("parseDataFrom_PARAGRAPH: parsed data : ${paragraph}");

			switch(key){
				case "min_2":
					data = paragraph.join("<span>&nbsp;&nbsp;|&nbsp;&nbsp;</span>");
					break;
				case "min_23":
					data = paragraph.join("<br>");
					break;
				default:
					data = paragraph.join("")
			}
		}

		return data;

	}

	public String parseDataFrom_MATRIX(NodeChild node, String key){
		log.info("Inside parseDataFrom_MATRIX for Id : ${key}");
		String data = "";
		if(node){
			for(row in node["MATRIX-ROW"]){
				String rowData = """<tr>
                      <td>${row.@YAXISPROMPT}</td>
                      <td>${row.text()}</td>
                  </tr>""";
				data = data + rowData;
			}
			return data;
		}
		println data
		return data;

	}

	public String parseDataFrom_GRID(NodeChild node, String key){
		log.info("Inside parseDataFrom_GRID for Id : ${key}");
		String data = "";
		Boolean dataExists = false;
		if(node){
			for(row in node["GRID-ROW"]){
				String completeString = "";
				String rowData = """<tr>""";
				for(column in row["GRID-COL"]){
					if(column.text() && column.text() != "")
					dataExists = true;
					completeString = completeString + column.text();
					rowData = rowData + """ <td>${column.text()} </td> """
				}
				rowData = rowData+  """</tr>""";
				if(completeString && completeString.trim() != "")
				data = data + rowData;
			}
		}
		if(!dataExists)
			data = "";
		return data;
	}

	public String parseDataFrom_DATE_TIME_NUMERIC(NodeChild node, String key){
		log.info("Inside parseDataFrom_DATE_TIME_NUMERIC for Id : ${key}");
		String data = ""
		if(node){
			String prompt = node.@PROMPT;
			String unit = node.@UNIT;
			String value = node.@VALUE;
			if(value?.trim())
				data = value+" "+unit;
		}
		return data;

	}

	String parseDataFrom_LONG_TEXT(NodeChild node, String key){
		log.info("Inside parseDataFrom_LONG_TEXT for Id : ${key}");
		String data = "";
		if(node)
			data = node?.text()?.trim();
		return data;
	}

	String parseDataFrom_SHORT_TEXT(NodeChild node, String key){
		log.info("Inside parseDataFrom_SHORT_TEXT for Id : ${key}");
		String data = "";
		if(node)
			data = node?.text()?.trim();
		return data;
	}
    void medantaDOAndHl7UpdateCusomJobService(def searchQueryVar, Map customJobDO, def accessionExist = false){
		List status = customJobDO?.get("status")
		List category = customJobDO?.get("categories")

        log.info("Inside MedantaEncounterUpdateInDOAndHl7CustomJobService searchQueryVar: "+ searchQueryVar)

		DBCollection diagnosticOrder = collection("diagnosticOrder");

		BasicDBObject query = new BasicDBObject("extension.value.category",new BasicDBObject("\$in":category))
		query.append("status.value", new BasicDBObject("\$in",status))
		if(accessionExist){
            query.append("identifier.value.value", new BasicDBObject("\$in",searchQueryVar))
		}else{
            query.append("event.2.dateTime.value",new BasicDBObject("\$gte",searchQueryVar))
		}
		query.append("extension.value.encounterUpdateStatus",new BasicDBObject("\$exists",false))
		BasicDBObject finalMatchQuery = new BasicDBObject("\$match",query)

		BasicDBObject groupConstraints = new BasicDBObject("encounter","\$enc._id")
		groupConstraints.append("patientVisit","\$enc.identifier.value.value")
		groupConstraints.append("DO","\$_id")
		groupConstraints.append("OrderID","\$identifier.value.value")
		groupConstraints.append("PatientId","\$extension.value.patientId")
		groupConstraints.append("accession","\$identifier.value.value")
		groupConstraints.append("Count", new BasicDBObject("\$sum",1))

		BasicDBObject group = new BasicDBObject("\$group",new BasicDBObject("_id",groupConstraints));

		BasicDBObject lookupEntries = new BasicDBObject("from","encounter")
		lookupEntries.append("localField","encounter")
		lookupEntries.append("foreignField","_id")
		lookupEntries.append("as","enc")

		BasicDBObject lookup = new BasicDBObject("\$lookup",lookupEntries)

		BasicDBObject unwind = new BasicDBObject("\$unwind","\$enc");
		BasicDBObject afterMatch = new BasicDBObject("\$match",new BasicDBObject("enc.extension.value.diagnosticOrder.Radiology","0"))

		def result = aggregateSearch(finalMatchQuery, group, lookup, unwind, afterMatch, diagnosticOrder)

		if(result.matchCount == 0){
			log.info("No result found on Aggregate search in MedantaEncounterUpdateInDOAndHl7CustomJobService: " + result)
		}else{
			log.info("${result.matchCount} Found in Aggregate search MedantaEncounterUpdateInDOAndHl7CustomJobService | " + result)
			for(DBObject obj: result.objects){
				log.info("DBObject MedantaEncounterUpdateInDOAndHl7CustomJobService: " + obj)

				Map mapObject = obj.get("_id")
				long dOId = mapObject.get("DO")
				String accessionId = mapObject.get("accession")[0] ;
				String patientVisitNumber = mapObject.get("patientVisit")[0] ;

				log.info("MedantaEncounterUpdateInDOAndHl7CustomJobService DOId ${dOId} accessionId ${accessionId} patientVisitNumber ${patientVisitNumber}")

				String collectionName = "diagnosticOrder";
				String hl7collectionName = "hl7Message";
				String sortMap = ""
				String searchMap=""
				int pageSize = 1
				int offset = 0
				Map searchResult = [:]

				String UpSearch = ""
				String updateMap = ""
				boolean upsert = false;
				boolean multi = false;
				boolean initialSuccess =false;

				try{
					UpSearch = '{"_id":'+dOId+'}';
					updateMap = '{"$unset":{"encounter":""}}'
					log.info("UpdateQuery param for ${collectionName}: upsearch: ${UpSearch}|| UpdatedMap: ${updateMap}")
					initialSuccess =  update(UpSearch, updateMap, collectionName, upsert, multi)

					if(initialSuccess){
						log.info("Updated ${collectionName}  MedantaEncounterUpdateInDOAndHl7CustomJobService with status: " + initialSuccess)
						try{
							updateMap = '{"$set":{"extension.0.value.0.encounterUpdateStatus":true}}';
							boolean initialSuccessExtension =false;
							log.info("UpdateQuery param to add booleanFlag encounterUpdateStatus upsearch: ${UpSearch} UpdateMap: ${updateMap}")
							log.info("Updating Diagnostic order with boolean flag: MedantaEncounterUpdateInDOAndHl7CustomJobService")
							initialSuccessExtension =  update(UpSearch, updateMap, collectionName, upsert, multi)
							if(initialSuccessExtension){
								log.info("DiagnosticOrder collection updated with boolean flag: MedantaEncounterUpdateInDOAndHl7CustomJobService")
								try{
									searchMap = '{"messageType":"ORU","patientVisitNumber":"'+patientVisitNumber+'","accession":"'+ accessionId +'","processed":"PROCESSED"}'
									sortMap = "{'_id':-1}"
									log.info("SearchQuery for ${hl7collectionName} searchMap: ${searchMap} || sortMap: ${sortMap}")
									searchResult = search(searchMap, hl7collectionName, sortMap,pageSize, offset)
									if (searchResult.matchCount == 0) {
										log.info ("[Message: MedantaEncounterUpdateInDOAndHl7CustomJobService Nothing in ${hl7collectionName}. No data found for processing." )
									}
									else {
										log.info("Message: MedantaEncounterUpdateInDOAndHl7CustomJobService ${hl7collectionName}. Data found for processing.")
										List<DBObject> dbObjects = searchResult.objects;
										long hl7MessageId = dbObjects[0]?.get("_id")
										if(hl7MessageId){
											UpSearch = '{"_id":'+hl7MessageId+'}';
											updateMap = '{"$set":{"processed":"PENDING"}}'
											boolean initialSuccessHl7 =false;
											log.info("UpdateQuery for ${hl7collectionName} Upsearch: ${UpSearch} || UpdateMap: ${updateMap}")
											initialSuccessHl7 =  update(UpSearch, updateMap, hl7collectionName, upsert, multi)
											if(initialSuccessHl7){
												log.info("Updated ${hl7collectionName} of field processed with Pending: MedantaEncounterUpdateInDOAndHl7CustomJobService")
											}else{
												log.info("${hl7collectionName} not updated of field processed with Pending: MedantaEncounterUpdateInDOAndHl7CustomJobService")
											}
										}
									}
								}catch(Exception exc){
									log.info("Exception occured in hl7Message collection")
								}
							}else{
								log.info("Not able to update DiagnosticOrder collection: MedantaEncounterUpdateInDOAndHl7CustomJobService")
							}
						}catch(Exception exc){
							log.info("Exception occured while updating ${collectionName} for booleanFlag");
						}
					}else{
						log.info("${collectionName} not updated MedantaEncounterUpdateInDOAndHl7CustomJobService")
					}
				}catch(Exception exc){
					log.info("Exception occured ${collectionName}: " + exc)
				}

			}
		}

	}

	public def aggregateSearch(BasicDBObject finalMatchQuery, BasicDBObject group, BasicDBObject lookup, BasicDBObject unwind, BasicDBObject afterMatch,DBCollection diagnosticOrder){
		log.info("Inside MedantaEncounterUpdateInDOAndHl7CustomJobService aggregateSearch")
		Map searchResult = [:]
		List<DBObject> aggObjects = []
		aggObjects.add(finalMatchQuery)
		aggObjects.add(lookup)
		aggObjects.add(unwind)
		aggObjects.add(afterMatch)
		aggObjects.add(group)

		log.info("Aggregate search object: " + aggObjects)
		List<DBObject> objects = []
		int matchCount = 0
		try {
			def totalCount = diagnosticOrder.count(finalMatchQuery.get("\$match"))
			int batchSize = 100
			int skip = 0;

			if (totalCount <= 100) {
				AggregationOutput aggregationOutput = diagnosticOrder.aggregate(aggObjects);
				aggregationOutput.results().each { o ->
					objects.add(o);
					matchCount++;
				}
			} else {
				boolean formSkipFlag = true
				for (int i = 0; i < (Math.round(Math.ceil(totalCount / batchSize))); i++) {
					if (formSkipFlag) {
						skip = 0
						formSkipFlag = false
					} else {
						skip = skip + batchSize
					}
					List<DBObject> innerAggObjects = []
					log.info("Inside MedantaEncounterUpdateInDOAndHl7CustomJobService- Before aggregate Query  ${i}:" + innerAggObjects)
					BasicDBObject sortQuery = new BasicDBObject("\$sort", new BasicDBObject("_id": -1))
					BasicDBObject limitQuery = new BasicDBObject("\$limit", batchSize)
					BasicDBObject skipQuery = new BasicDBObject("\$skip", skip)
					innerAggObjects.add(sortQuery)
					innerAggObjects.add(finalMatchQuery)
					innerAggObjects.add(lookup)
					innerAggObjects.add(unwind)
					innerAggObjects.add(afterMatch)
					innerAggObjects.add(group)
					innerAggObjects.add(skipQuery)
					innerAggObjects.add(limitQuery)
					log.info("Inside MedantaEncounterUpdateInDOAndHl7CustomJobService- After aggregate Query  ${i}:" + innerAggObjects)
					AggregationOutput aggregationOutput = diagnosticOrder.aggregate(innerAggObjects);
					aggregationOutput.results().each { o ->
						objects.add(o);
						matchCount++;
					}
				}
			}

			searchResult.objects = objects;
			searchResult.matchCount = matchCount;
		}catch(Exception exc){
			log.info("Exception occured in Aggregate Search")
			log.debug("[ERROR] | MedantaEncounterUpdateInDOAndHl7CustomJobService | aggregatSearch : "+ex)
		}

		log.debug("MedantaEncounterUpdateInDOAndHl7CustomJobService | aggregatSearch searchResult "+searchResult)
		return searchResult
	}

	def update(def query, def update, String collectionName, Boolean upsert = false, Boolean multi = false) {
		boolean isSuccess = false;

		DBCollection collection = collection(collectionName);
		DBObject queryObj = (DBObject) JSON.parse(query.toString())
		DBObject updateObj = (DBObject) JSON.parse(update.toString())
		log.info("### Inside update method: ${queryObj} ### ${updateObj}")
		try {
			collection.update(queryObj, updateObj, upsert, multi, WriteConcern.SAFE)
			isSuccess = true
		} catch (MongoException ex) {
			isSuccess = false
			log.error("Exception in updating object ${query.toString()} while updating with ${update.toString()}", ex)
		}

		return isSuccess
	}
	def saveSignature(def row, def docType) {
		DBCollection practitionerSignatureColl = db.getCollection("practitionerSignature")
		String fileLocation = defaultPractitionerSignLocation;

		if (docType.equals("verifieddoc")) {
			if (row.verifieddocsign && row.LabDoctorID) {
				String signPath = "";
				boolean isExists = false;
				(isExists, signPath) = isPractitionerExists(row, 'verifieddoc', practitionerSignatureColl);
				if (!isExists) {
					signPath = fileLocation + row.LabDoctorID + "_" + row.verifieddocmodifieddate + ".PNG"
					convertBlobToFile(fileLocation, signPath, row.verifieddocsign)

					BasicDBObject insertObj = new BasicDBObject();
					insertObj.append("practitionerId", row.LabDoctorID)
					insertObj.append("practitionerName", row.LabDoctorName)
					insertObj.append("positionCode", row.verifieddocpositioncode)
					insertObj.append("practitionerSignPath", signPath)
					insertObj.append("positionDesc", row.LabDoctorDesignation)
					insertObj.append("modifiedDate", row.verifieddocmodifieddate)
					insertObj.append("dateCreated", new Date())

					practitionerSignatureColl.insert(insertObj, WriteConcern.SAFE)
					return signPath;
				} else return signPath;
			} else return "";

		}
	}


	def isPractitionerExists(def row, def practitionerType, def practitionerSignatureColl) {
		if (practitionerType.equals("verifieddoc")) {
			def doc = practitionerSignatureColl.find(new BasicDBObject('practitionerId', row.LabDoctorID)).sort(new BasicDBObject('dateCreated', -1)).limit(1)
			if (doc.hasNext()) {
				def practitioner = doc.next()
				String modifiedDate = practitioner['modifiedDate']

				if (!modifiedDate.equals(row.verifieddocmodifieddate)) {
					String base64 = convertBlobToBase64(row.verifieddocsign)
					String preBase64 = convertFileToBase64(practitioner['practitionerSignPath'])
					if (base64.equals(preBase64)) {
						if(Long.parseLong(row.verifieddocmodifieddate)> Long.parseLong(modifiedDate)){
							practitionerSignatureColl.update(new BasicDBObject("_id", practitioner._id), new BasicDBObject().append("\$set", new BasicDBObject().append("modifiedDate", row.verifieddocmodifieddate)), false, false, WriteConcern.SAFE)

						}
						return [true, practitioner['practitionerSignPath']]
					} else return [false, ""]

				} else return [true, practitioner['practitionerSignPath']]

			} else return [false, ""]
		}
	}

	def convertBlobToBase64(def blob) {
		InputStream stream = blob.getBinaryStream();

		int bufLength = 2048;
		byte[] buffer = new byte[2048];
		byte[] data;

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		int readLength;
		while ((readLength = stream.read(buffer, 0, bufLength)) != -1) {
			out.write(buffer, 0, readLength);
		}

		data = out.toByteArray();

		String imageString = Base64.getEncoder().withoutPadding().encodeToString(data);
		return imageString;

	}

	def convertFileToBase64(def imgPath){

		FileInputStream stream = new FileInputStream(imgPath);
		// get byte array from image stream
		int bufLength = 2048;
		byte[] buffer = new byte[2048];
		byte[] data;

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		int readLength;
		while ((readLength = stream.read(buffer, 0, bufLength)) != -1) {
			out.write(buffer, 0, readLength);
		}

		data = out.toByteArray();
		String imageString = Base64.getEncoder().withoutPadding().encodeToString(data);
		return imageString;
	}

	def convertBlobToFile(def fileLocation, def signPath, def HIST_DATA) {
		File path = new File(fileLocation)
		if (!(path.exists())) {
			path.mkdirs()
		}
		File blobFile = new File(signPath);


		FileOutputStream outStream = new FileOutputStream(blobFile);
		InputStream inStream = HIST_DATA.getBinaryStream();

		int length = -1;
		int size = HIST_DATA.getBufferSize();
		byte[] buffer = new byte[size];

		while ((length = inStream.read(buffer)) != -1) {
			outStream.write(buffer, 0, length);
			outStream.flush();
		}
		inStream.close();
		outStream.close();
	}

	//Edited by Akhil

	def getHisCreds()
	{
		Map dBMap = [:]
		def instance = grailsApplication.config.MedantaOraclDB.instance
		def userName = grailsApplication.config.MedantaOraclDB.user
		def passKey = grailsApplication.config.MedantaOraclDB.password
		def driver = grailsApplication.config.MedantaOraclDB.driver

		dBMap.put("instance", instance)
		dBMap.put("user", userName)
		dBMap.put("password", passKey)
		dBMap.put("driver", driver)
		return dBMap;
	}


	def getSqlHISConnection()
	{
		def sql
		Map dBMap =  getHisCreds();
		try {
			sql = Sql.newInstance(dBMap.get("instance"), dBMap.get("user"), dBMap.get("password"), dBMap.get("driver"))
		}catch(SQLException ex)
		{
			log.info("\n Caught an Exception '${ex}'")
		}
		return sql
	}

	def dumpPatLivSyncData(def query) {
		def CollectionName = "patLiveSync"
		DBCollection patliveSync = collection(CollectionName);
		def sucFlag = false
		def counter = 0
		def columnNames = [
				"patientId",
				"namePrefix",
				"firstName",
				"secondName",
				"familyName",
				"sex",
				"dateOfBirth",
				"contact1No",
				"contact2No",
				"emailId",
				"modifiedDate",
				"ADDR1_LINE1",
				"ADDR1_LINE2",
				"ADDR1_LINE3",
				"ADDR1_LINE4",
				"POSTAL1_CODE",
				"COUNTRY1_CODE"
		];



		def sql = getSqlHISConnection()
		if(sql != null)
		{
			sql.eachRow(query) {
				row ->
					String patId = row.patientId
					if (patId.find(/(?i)^\s*(GG)\.*/))
					{
						log.info("Patient record found with  patientId starting with GG: " + patId + ".Skipping this record !!\n\n");
						return
					}
					//  Map<String, Object> documentMap = new HashMap<String, Object>()
					BasicDBObject myUpdateobj = new BasicDBObject()
					columnNames.each()
							{
								def val = row."${it.toUpperCase()}"
								if(!val) {
									// myUpdateobj.put(val,"")
								}else {
									val = val.toString();
									String updateVal = val.replaceAll("\\P{Print}", "");
									myUpdateobj.put(it, updateVal.toString());
								}
							}
					myUpdateobj.put("lastUpdated",new Date())
					sucFlag = patliveSync.insert(myUpdateobj);
					if(sucFlag) {
						counter++
					}

			}
		}
		sql.close()
		return counter
	}

	def patLivSyncBaseQuery()
	{
		def baseQuery = """select P.patient_id patientId , P.name_prefix namePrefix, P.FIRST_NAME firstName,
                                            P.SECOND_NAME secondName, P.FAMILY_NAME familyName, P.sex,
                                            to_char(P.DATE_OF_BIRTH , 'YYYYMMDD') dateOfBirth, P.CONTACT1_NO contact1No,
                                            P.CONTACT2_NO contact2No, P.EMAIL_ID emailId, P.MODIFIED_DATE modifiedDate,
                                            A.ADDR1_LINE1 ADDR1_LINE1, A.ADDR1_LINE2 ADDR1_LINE2, A.ADDR1_LINE3 ADDR1_LINE3, 
                                            A.ADDR1_LINE4 ADDR1_LINE4, A.POSTAL1_CODE POSTAL1_CODE, A.COUNTRY1_CODE COUNTRY1_CODE
                                            from mp_patient P, MP_PAT_ADDRESSES A
                                            where P.patient_id = A.patient_id"""
		return baseQuery;
	}

}


