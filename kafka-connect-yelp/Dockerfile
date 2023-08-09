FROM cnfldemos/cp-server-connect-datagen:0.5.3-7.1.0
USER root:root
COPY ./target/lib /usr/share/java/yelp-df-connector/lib
COPY ./yelp_academic_dataset_business.json /data/yelp/yelp_academic_dataset_business.json
USER 1001