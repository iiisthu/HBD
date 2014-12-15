#import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

sc = SparkContext(appName="StoreDataIntoSparkSQL")
sqlContext = SQLContext(sc)

print "Hello!"


path = "/app/sys/ra/wgs"
lines = sc.textFile(path+"/small_data_head")
#lineLengths = lines.map(lambda s: len(s))
#totalLength = lineLengths.reduce(lambda a, b: a + b)

parts = lines.map(lambda l: l.replace(" ", ":").replace("\t", ":").split(":"))
#print "The first element of this RDD:", parts.first()

# Below lists all kinds of data records
# model parts.map(lambda p: Row(=p[], =p[], =p[], =p[], =p[], =p[], =p[], =p[], =p[], =p[]))

#cpu_basic
#host_sn?!
logs = parts.map(lambda p: Row(host=p[1], cpu_id=p[2], value=p[3], core_n=p[5], freq=p[7], l2_c=p[9], l3_c=p[11], manufacturer=p[13], sn=p[15]))

#cpu_failure
parts.map(lambda p: Row(host=p[1], cpu_id=p[2], value=p[3], host_sn=p[5], reason=p[7], record_id=p[9], result=p[11], source=p[13]))

#cpu_performance
parts.map(lambda p: Row(host=p[1], cpu_id=p[2], value=p[3], host_sn=p[5], cur_freq=p[7], ht=p[9], norm=p[11], pstate=p[13], record_id=p[15], turbo=p[17])

#ctrl_basic
parts.map(lambda p: Row(host=p[1], ctrl_id=p[2], value=p[3], host_sn=p[5], firmware=p[7], manufacturer=p[9], model=p[11], sn=p[13])

#ctrl_failure
parts.map(lambda p: Row(host=p[1], ctrl_id=p[2], value=p[3], host_sn=p[5], reason=p[7], record_id=p[9], result=p[11], source=p[13]))

#ctrl_status
parts.map(lambda p: Row(host=p[1], ctrl_id=p[2], value=p[3], host_sn=p[5], bcp=p[7], epc=p[9], record_id=p[11], temp=p[13]))

#custom_has_config
parts.map(lambda p: Row(host=p[1], value=p[3], monitor=p[5], power=p[7], predict=p[9], prepair=p[11], service_down=p[13], service_up=p[15], soa=p[17], version=p[19]))

#dimm_basic
parts.map(lambda p: Row(host=p[1], dimm_id=p[3], host_sn=p[5], capacity=p[7], freq=p[9], logicalslot=p[11], manufacturer=p[13], model=p[15], slot=p[17], sn=p[19], volt=p[21]))

#dimm_failure
parts.map(lambda p: Row(host=p[1], dimm_id=p[2], value=p[3], host_sn=p[5], reason=p[7], record_id=p[9], result=p[11], source=p[13]))

#dimm_performance
parts.map(lambda p: Row(host=p[1], dimm_id=p[2], value=p[3], host_sn=p[5], curr_freq=p[7], numa=p[9], record_id=p[11]))

#hdd_basic
parts.map(lambda p: Row(host=p[1], pd_id=p[2], value=p[3], host_sn=p[5], capacity=p[7], did=p[9], firmware=p[11], interface=p[13], lss=p[15], manufacturer=p[17], media=p[19], model=p[21], pss=p[23], rpm=p[25], sn=p[27]))

#hdd_failure
parts.map(lambda p: Row(host=p[1], pd_id=p[2], value=p[3], host_sn=p[5], reason=p[7], record_id=p[9], result=p[11], source=p[13]))

#hdd_power
parts.map(lambda p: Row(host=p[1], device_id=p[2], value=p[3], host_sn=p[5], epc=p[7], mode=p[9], record_id=p[11]))

#hdd_smart
#???
parts.map(lambda p: Row(host=p[1], device_id=p[2], value=p[3], l_raw=p[5], l_thresh=p[7], l_worst=p[9], 1l=p[], =p[], =p[], =p[]))

#hdd_status
parts.map(lambda p: Row(host=p[1], device_id=p[2], value=p[3], host_sn=p[5], capacity=p[7], did=p[9], firmware=p[11], interface=p[13], lss=p[15], manufacturer=p[17], media=p[19], model=p[21], pss=p[23], rpm=p[25], sn=p[27]))

#nic_basic
parts.map(lambda p: Row(host=p[1], eth_id=p[2], value=p[3], host_sn=p[5], bandwidth=p[7], manufacurer=p[9], sn=p[11], version=p[13]))

#nic_failure
parts.map(lambda p: Row(host=p[1], eth_id=p[2], value=p[3], host_sn=p[5], reason=p[7], record_id=p[9], result=p[11], source=p[13]))

#nic_status
parts.map(lambda p: Row(host=p[1], eth_id=p[2], value=p[3], host_sn=p[5], duplex=p[7], phyad=p[9], port=p[11], record_id=p[13], speed=p[15]))

#psu_basic
parts.map(lambda p: Row(host=p[1], psu_id=p[2], value=p[3], host_sn=p[5], id=p[7], model=p[9]))

#server_basic
parts.map(lambda p: Row(host=p[1], value=p[3], host_sn=p[5], dimm_mn=p[7], hotplug=p[9], manufacturer=p[11], model=p[13], online_time=p[15], sn=p[17]))

#server_health
parts.map(lambda p: Row(host=p[1], value=p[3], host_sn=p[5], code=p[7]))

#server_overall_status
#??? what is the host
parts.map(lambda p: Row(host=p[1], value=p[3], avg_power=p[5], avt_temp=p[7], bios_v=p[9], bmc_v=p[11], capping_power=p[13], capping_temp=p[15], cpu_n=p[17], epc=p[19], hdoctor_v=p[21], mb_v=p[23], peak_power=p[25], peak_temp=p[27], procuct=p[29], rack_info=p[31], record_id=p[33]))

#server_rt_status
parts.map(lambda p: Row(host=p[1], value=p[3], host_sn=p[5], cpupower=p[7], dimmpower=p[9], in_temp=p[11], out_temp=p[13], pch_temp=p[15], power=p[17], power_ratio=p[19], record_id=p[21]))

#vd_failure
parts.map(lambda p: Row(host=p[1], vd_id=p[2], value=p[3], reason=p[5], record_id=p[7], result=p[9], source=p[11]))

#vd_status
parts.map(lambda p: Row(host=p[1], vd_id=p[2], value=p[3], host_sn=p[5], label=p[7], level=p[9], pd_n=p[11], pid=p[13], rcp=p[15], record_id=p[17], rw_r=p[19], status=p[21], tid=p[23], wcp=p[25]))



schema_cpu_basic = sqlContext.inferSchema(logs)
schema_cpu_basic.registerTempTable("logs")

#some_cpu = sqlContext.sql("SELECT core_n FROM logs")
#result = some_cpu.map(lambda p: "host: " + p.core_n)
#for x in result.collect():
#	print x

#schema_cpu_basic.saveAsTextFile(path + "/output")
schema_cpu_basic.saveAsParquetFile("/app/sys/ra/wgs/output")


print "!!!!!!!!!!!!!!!!!!!"

sc.stop()
