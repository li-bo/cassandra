/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.compaction;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class TimeTieredCompactionStrategyOptions
{
    private static final Logger logger = LoggerFactory.getLogger(TimeTieredCompactionStrategyOptions.class);
    protected static final TimeUnit DEFAULT_TIMESTAMP_RESOLUTION = TimeUnit.MICROSECONDS;
    // make min_window 1 minutes
    protected static final TimeUnit DEFAULT_COMPACTION_WINDOW_UNIT = TimeUnit.MINUTES;
    protected static final long DEFAULT_COMPACTION_WINDOW_SIZE_MIN = 90;
    protected static final long DEFAULT_COMPACTION_WINDOW_SIZE_MAX = 1440;
    protected static final long DEFAULT_REMOVE_SIZE = 14400;
    protected static final long DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS = 60 * 10;
    protected static final int DEFAULT_BUFFER_COMPACTION_SIZE = 10;

    protected static final String TIMESTAMP_RESOLUTION_KEY = "timestamp_resolution";
    protected static final String COMPACTION_WINDOW_UNIT_KEY = "compaction_window_unit";
    protected static final String COMPACTION_WINDOW_SIZE_MIN_KEY = "compaction_window_size_min";
    protected static final String COMPACTION_WINDOW_SIZE_MAX_KEY = "compaction_window_size_max";
    protected static final String REMOVE_SIZE_KEY = "remove_window_size";
    protected static final String EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY = "expired_sstable_check_frequency_seconds";
    protected static final String BUFFER_COMPACTION_SIZE = "buffer_compaction_size";

    protected final TimeUnit timestampResolution;
    protected final TimeUnit sstableWindowUnit;
    protected final long sstableWindowSizeMin;
    protected final long sstableWindowSizeMax;
    protected final long removeWindowSize;
    protected final long expiredSSTableCheckFrequency;
    protected final int bufferCompactionSize;


    public TimeTieredCompactionStrategyOptions(Map<String, String> options)
    {
        long sstableWindowSizeMax1;
        long removeWindowSize1;

        String optionValue = options.get(TIMESTAMP_RESOLUTION_KEY);
        timestampResolution = optionValue == null ? DEFAULT_TIMESTAMP_RESOLUTION : TimeUnit.valueOf(optionValue);
        if (timestampResolution != DEFAULT_TIMESTAMP_RESOLUTION)
            logger.warn("Using a non-default timestamp_resolution {} - are you really doing inserts with USING TIMESTAMP <non_microsecond_timestamp> (or driver equivalent)?", timestampResolution.toString());

        optionValue = options.get(COMPACTION_WINDOW_UNIT_KEY);
        sstableWindowUnit = optionValue == null ? DEFAULT_COMPACTION_WINDOW_UNIT : TimeUnit.valueOf(optionValue);

        optionValue = options.get(COMPACTION_WINDOW_SIZE_MIN_KEY);
        sstableWindowSizeMin = timestampResolution.convert(optionValue == null ? DEFAULT_COMPACTION_WINDOW_SIZE_MIN : Long.parseLong(optionValue), sstableWindowUnit);

        optionValue = options.get(COMPACTION_WINDOW_SIZE_MAX_KEY);
        sstableWindowSizeMax1 = timestampResolution.convert(optionValue == null ? DEFAULT_COMPACTION_WINDOW_SIZE_MAX : Long.parseLong(optionValue), sstableWindowUnit);
        if (sstableWindowSizeMax1 < sstableWindowSizeMin)
            sstableWindowSizeMax1 = sstableWindowSizeMin;
        sstableWindowSizeMax = sstableWindowSizeMax1;

        optionValue = options.get(REMOVE_SIZE_KEY);
        removeWindowSize1 = timestampResolution.convert(optionValue == null ? DEFAULT_REMOVE_SIZE : Long.parseLong(optionValue), sstableWindowUnit);
        if (removeWindowSize1 < sstableWindowSizeMax)
        removeWindowSize1 = sstableWindowSizeMax;
        removeWindowSize = removeWindowSize1;

        optionValue = options.get(EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY);
        expiredSSTableCheckFrequency = TimeUnit.MILLISECONDS.convert(optionValue == null ? DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS : Long.parseLong(optionValue), TimeUnit.SECONDS);

        optionValue = options.get(BUFFER_COMPACTION_SIZE);
        bufferCompactionSize = optionValue == null ? DEFAULT_BUFFER_COMPACTION_SIZE : Integer.parseInt(optionValue);
    }

    public TimeTieredCompactionStrategyOptions()
    {
        timestampResolution = DEFAULT_TIMESTAMP_RESOLUTION;
        sstableWindowUnit = DEFAULT_COMPACTION_WINDOW_UNIT;
        sstableWindowSizeMin = TimeUnit.MILLISECONDS.convert(DEFAULT_COMPACTION_WINDOW_SIZE_MIN, sstableWindowUnit);
        sstableWindowSizeMax = TimeUnit.MILLISECONDS.convert(DEFAULT_COMPACTION_WINDOW_SIZE_MAX, sstableWindowUnit);
        removeWindowSize = TimeUnit.MILLISECONDS.convert(DEFAULT_REMOVE_SIZE, sstableWindowUnit);;
        expiredSSTableCheckFrequency = TimeUnit.MILLISECONDS.convert(DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS, TimeUnit.SECONDS);
        bufferCompactionSize = DEFAULT_BUFFER_COMPACTION_SIZE;
    }

    public static Map<String, String> validateOptions(Map<String, String> options, Map<String, String> uncheckedOptions) throws  ConfigurationException
    {
        String optionValue = options.get(TIMESTAMP_RESOLUTION_KEY);
        try
        {
            if (optionValue != null)
                TimeUnit.valueOf(optionValue);
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException(String.format("timestamp_resolution %s is not valid", optionValue));
        }


        optionValue = options.get(COMPACTION_WINDOW_SIZE_MIN_KEY);
        try
        {
            long minWindowSize = optionValue == null ? DEFAULT_COMPACTION_WINDOW_SIZE_MIN : Integer.parseInt(optionValue);
            if (minWindowSize <= 0)
            {
                throw new ConfigurationException(String.format("%s must be greater than 0, but was %d", DEFAULT_COMPACTION_WINDOW_SIZE_MIN, minWindowSize));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, DEFAULT_COMPACTION_WINDOW_SIZE_MIN), e);
        }

        optionValue = options.get(COMPACTION_WINDOW_SIZE_MAX_KEY);
        try
        {
            long maxWindowSize = optionValue == null ? DEFAULT_COMPACTION_WINDOW_SIZE_MAX : Integer.parseInt(optionValue);
            if (maxWindowSize <= 0)
            {
                throw new ConfigurationException(String.format("%s must be greater than 0, but was %d", COMPACTION_WINDOW_SIZE_MAX_KEY, maxWindowSize));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, COMPACTION_WINDOW_SIZE_MAX_KEY), e);
        }


        optionValue = options.get(EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY);
        try
        {
            long expiredCheckFrequency = optionValue == null ? DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS : Long.parseLong(optionValue);
            if (expiredCheckFrequency < 0)
            {
                throw new ConfigurationException(String.format("%s must not be negative, but was %d", EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY, expiredCheckFrequency));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY), e);
        }


        optionValue = options.get(REMOVE_SIZE_KEY);
        try
        {
            long removeWindowSize = optionValue == null ? DEFAULT_REMOVE_SIZE : Integer.parseInt(optionValue);
            if (removeWindowSize <= 0)
            {
                throw new ConfigurationException(String.format("%s must be greater than 0, but was %d", REMOVE_SIZE_KEY, removeWindowSize));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, REMOVE_SIZE_KEY), e);
        }

        optionValue = options.get(BUFFER_COMPACTION_SIZE);
        try
        {
            int bufferWindowSize = optionValue == null ? DEFAULT_BUFFER_COMPACTION_SIZE : Integer.parseInt(optionValue);
            if (bufferWindowSize <= 0)
            {
                throw new ConfigurationException(String.format("%s must be greater than 0, but was %d", REMOVE_SIZE_KEY, bufferWindowSize));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, BUFFER_COMPACTION_SIZE), e);
        }

        uncheckedOptions.remove(TIMESTAMP_RESOLUTION_KEY);
        uncheckedOptions.remove(COMPACTION_WINDOW_UNIT_KEY);
        uncheckedOptions.remove(COMPACTION_WINDOW_SIZE_MIN_KEY);
        uncheckedOptions.remove(COMPACTION_WINDOW_SIZE_MAX_KEY);
        uncheckedOptions.remove(EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY);
        uncheckedOptions.remove(REMOVE_SIZE_KEY);
        uncheckedOptions.remove(BUFFER_COMPACTION_SIZE);

        return uncheckedOptions;
    }
}
