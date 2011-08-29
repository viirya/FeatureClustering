
OUTPUT_DIR=bin
OUTPUT_JAR=build/FeatureClustering.jar
SRC = org/viirya/graph/*.java
JAVA_DEP = /usr/lib/hadoop/hadoop-0.18.3-6cloudera0.3.0-core.jar:.

all: ${SRC}
	rm -rf ${OUTPUT_DIR}
	mkdir ${OUTPUT_DIR}
	javac -classpath ${JAVA_DEP} ${SRC} -d ${OUTPUT_DIR}
	jar -cfv ${OUTPUT_JAR} -C ${OUTPUT_DIR} .

clean:
	hadoop dfs -rmr output/feature_clu_data

#	hadoop dfs -rmr output/graph_data/adl
#	hadoop dfs -rm output/graph_data/adl_output
#	hadoop dfs -rm output/graph_data/images_number_output
#	hadoop dfs -rmr output/graph_data/tf
#	hadoop dfs -rmr output/graph_data/idf
#	hadoop dfs -rmr output/graph_data/inverted_list
#	hadoop dfs -rmr output/graph_data/graph
    
run:
	hadoop jar ${OUTPUT_JAR} org.viirya.graph.FeatureClustering data/flickr550/clusters/flickr550.textual/threshold0.005/Output data/flickr550/features/flickr550.full_size_HA_1M_vw_by_flicrk11k compress

#data/flickr550/clusters/flickr550.graph.full_size_HA_1M_vw_by_flicrk11k/threshold0.002/graph_threshold0.002/Output data/flickr550/features/flickr550.full_size_HA_1M_vw_by_flicrk11k compress

#data/flickr550/features/flickr550.textual 0.005 compress

#data/flickr550/features/Flickr550_psedoobj_normalized 0.01 compress

#data/Flickr550_psedoobj_normalized 0.005 compress

#data/flickr550.full_size_HA_1M_vw_by_flicrk11k 0.002 compress
