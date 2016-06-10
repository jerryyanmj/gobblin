/*
 * Copyright (C) 2014-2016 NerdWallet All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.example.twc;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.kafka.KafkaSource;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A {@link KafkaSource} implementation for SimpleKafkaExtractor.
 *
 * @author akshay@nerdwallet.com
 */
public class TWCConopsKafkaAvroSource extends KafkaSource<Schema, GenericRecord> {
    private final Logger log = LoggerFactory.getLogger(TWCConopsKafkaAvroSource.class);

    @Override
    public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state) throws IOException {
      log.debug("Return a new TWCConopsKafkaAvroExtractor");
      return new TWCConopsKafkaAvroExtractor(state);
    }
}
