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

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class TrueTimeWindowCompactionStrategyOptions
{
    private static final Logger logger = LoggerFactory.getLogger(TrueTimeWindowCompactionStrategyOptions.class);

    protected static final TimeUnit DEFAULT_TIMESTAMP_RESOLUTION = TimeUnit.MICROSECONDS;
    protected static final TimeUnit DEFAULT_COMPACTION_WINDOW_UNIT = TimeUnit.DAYS;
    protected static final int DEFAULT_COMPACTION_WINDOW_SIZE = 1;
    protected static final int DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS = 60 * 10;
    protected static final Boolean DEFAULT_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION = false;
    private static final long DEFAULT_SSTABLE_SPLIT_THRESHOLD = 128L;   //128MB
    private static final long DEFAULT_MAX_SSTABLE_SIZE_THRESHOLD = 10 * 1024L; //10GB
    private static final int DEFAULT_MIN_SSTABLE_SPLIT_WINDOW = 1;
    private static final long MEGABYTES = 1024*1024L;


    protected static final String TIMESTAMP_RESOLUTION_KEY = "timestamp_resolution";
    protected static final String COMPACTION_WINDOW_UNIT_KEY = "compaction_window_unit";
    protected static final String COMPACTION_WINDOW_SIZE_KEY = "compaction_window_size";
    protected static final String EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY = "expired_sstable_check_frequency_seconds";
    protected static final String UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_KEY = "unsafe_aggressive_sstable_expiration";
    protected static final String SSTABLE_SPLIT_THRESHOLD_KEY = "sstable_split_threshold";
    protected static final String MAX_SSTABLE_SIZE_THRESHOLD_KEY = "max_sstable_size_threshold";
    protected static final String MIN_SSTABLE_SPLIT_WINDOW_KEY = "min_sstable_split_window";

    static final String UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_PROPERTY = Config.PROPERTY_PREFIX + "allow_unsafe_aggressive_sstable_expiration";

    public final int sstableWindowSize;
    public final TimeUnit sstableWindowUnit;
    public final TimeUnit timestampResolution;
    protected final long expiredSSTableCheckFrequency;
    protected final boolean ignoreOverlaps;
    public final long sstableSplitThreshold;
    public final long maxSStableSizeThreshold;
    public final long timeWindowInMillis;
    public final int minSStableSplitWindow;

    SizeTieredCompactionStrategyOptions stcsOptions;

    protected final static ImmutableList<TimeUnit> validTimestampTimeUnits = ImmutableList.of(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, TimeUnit.MICROSECONDS, TimeUnit.NANOSECONDS);
    protected final static ImmutableList<TimeUnit> validWindowTimeUnits = ImmutableList.of(TimeUnit.MINUTES, TimeUnit.HOURS, TimeUnit.DAYS);

    public TrueTimeWindowCompactionStrategyOptions(Map<String, String> options)
    {
        String optionValue = options.get(TIMESTAMP_RESOLUTION_KEY);
        timestampResolution = optionValue == null ? DEFAULT_TIMESTAMP_RESOLUTION : TimeUnit.valueOf(optionValue);
        if (timestampResolution != DEFAULT_TIMESTAMP_RESOLUTION)
            logger.warn("Using a non-default timestamp_resolution {} - are you really doing inserts with USING TIMESTAMP <non_microsecond_timestamp> (or driver equivalent)?", timestampResolution.toString());

        optionValue = options.get(COMPACTION_WINDOW_UNIT_KEY);
        sstableWindowUnit = optionValue == null ? DEFAULT_COMPACTION_WINDOW_UNIT : TimeUnit.valueOf(optionValue);

        optionValue = options.get(COMPACTION_WINDOW_SIZE_KEY);
        sstableWindowSize = optionValue == null ? DEFAULT_COMPACTION_WINDOW_SIZE : Integer.parseInt(optionValue);

        optionValue = options.get(EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY);
        expiredSSTableCheckFrequency = TimeUnit.MILLISECONDS.convert(optionValue == null ? DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS : Long.parseLong(optionValue), TimeUnit.SECONDS);

        optionValue = options.get(UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_KEY);
        ignoreOverlaps = optionValue == null ? DEFAULT_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION : (Boolean.getBoolean(UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_PROPERTY) && Boolean.parseBoolean(optionValue));

        optionValue = options.get(SSTABLE_SPLIT_THRESHOLD_KEY);
        sstableSplitThreshold = optionValue == null ? DEFAULT_SSTABLE_SPLIT_THRESHOLD * MEGABYTES : Long.parseLong(optionValue) * MEGABYTES;

        optionValue = options.get(MAX_SSTABLE_SIZE_THRESHOLD_KEY);
        maxSStableSizeThreshold = optionValue == null ? DEFAULT_MAX_SSTABLE_SIZE_THRESHOLD * MEGABYTES : Long.parseLong(optionValue) * MEGABYTES;

        optionValue = options.get(MIN_SSTABLE_SPLIT_WINDOW_KEY);
        minSStableSplitWindow = optionValue == null ? DEFAULT_MIN_SSTABLE_SPLIT_WINDOW : Integer.parseInt(optionValue);

        timeWindowInMillis = getTimeWindowInMills(sstableWindowUnit, sstableWindowSize);

        stcsOptions = new SizeTieredCompactionStrategyOptions(options);
    }

    private long getTimeWindowInMills(TimeUnit windowTimeUnit, int windowTimeSize) {
        switch(windowTimeUnit) {
            case MINUTES:
                return 60L * windowTimeSize * 1000L;
            case HOURS:
                return 3600L * windowTimeSize * 1000L;
            case DAYS:
            default:
                return 86400L * windowTimeSize * 1000L;
        }
    }

    public TrueTimeWindowCompactionStrategyOptions()
    {
        sstableWindowUnit = DEFAULT_COMPACTION_WINDOW_UNIT;
        timestampResolution = DEFAULT_TIMESTAMP_RESOLUTION;
        sstableWindowSize = DEFAULT_COMPACTION_WINDOW_SIZE;
        expiredSSTableCheckFrequency = TimeUnit.MILLISECONDS.convert(DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS, TimeUnit.SECONDS);
        ignoreOverlaps = DEFAULT_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION;
        sstableSplitThreshold = DEFAULT_SSTABLE_SPLIT_THRESHOLD * MEGABYTES;
        maxSStableSizeThreshold = DEFAULT_MAX_SSTABLE_SIZE_THRESHOLD * MEGABYTES;
        minSStableSplitWindow = DEFAULT_MIN_SSTABLE_SPLIT_WINDOW;
        timeWindowInMillis = getTimeWindowInMills(sstableWindowUnit, sstableWindowSize);

        stcsOptions = new SizeTieredCompactionStrategyOptions();
    }

    public static Map<String, String> validateOptions(Map<String, String> options, Map<String, String> uncheckedOptions) throws  ConfigurationException
    {
        String optionValue = options.get(TIMESTAMP_RESOLUTION_KEY);
        try
        {
            if (optionValue != null)
                if (!validTimestampTimeUnits.contains(TimeUnit.valueOf(optionValue)))
                    throw new ConfigurationException(String.format("%s is not valid for %s", optionValue, TIMESTAMP_RESOLUTION_KEY));
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException(String.format("%s is not valid for %s", optionValue, TIMESTAMP_RESOLUTION_KEY));
        }


        optionValue = options.get(COMPACTION_WINDOW_UNIT_KEY);
        try
        {
            if (optionValue != null)
                if (!validWindowTimeUnits.contains(TimeUnit.valueOf(optionValue)))
                    throw new ConfigurationException(String.format("%s is not valid for %s", optionValue, COMPACTION_WINDOW_UNIT_KEY));

        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException(String.format("%s is not valid for %s", optionValue, COMPACTION_WINDOW_UNIT_KEY), e);
        }

        optionValue = options.get(COMPACTION_WINDOW_SIZE_KEY);
        try
        {
            int sstableWindowSize = optionValue == null ? DEFAULT_COMPACTION_WINDOW_SIZE : Integer.parseInt(optionValue);
            if (sstableWindowSize < 1)
            {
                throw new ConfigurationException(String.format("%d must be greater than 1 for %s", sstableWindowSize, COMPACTION_WINDOW_SIZE_KEY));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, COMPACTION_WINDOW_SIZE_KEY), e);
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


        optionValue = options.get(UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_KEY);
        if (optionValue != null)
        {
            if (!(optionValue.equalsIgnoreCase("true") || optionValue.equalsIgnoreCase("false")))
                throw new ConfigurationException(String.format("%s is not 'true' or 'false' (%s)", UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_KEY, optionValue));

            if(optionValue.equalsIgnoreCase("true") && !Boolean.getBoolean(UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_PROPERTY))
                throw new ConfigurationException(String.format("%s is requested but not allowed, restart cassandra with -D%s=true to allow it", UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_KEY, UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_PROPERTY));
        }

        optionValue = options.get(SSTABLE_SPLIT_THRESHOLD_KEY);
        try
        {
            long sstableSplitThreshold = optionValue == null ? DEFAULT_SSTABLE_SPLIT_THRESHOLD : Long.parseLong(optionValue);
            if (sstableSplitThreshold < 0)
            {
                throw new ConfigurationException(String.format("%d must be greater than 0 for %s", sstableSplitThreshold, SSTABLE_SPLIT_THRESHOLD_KEY));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, SSTABLE_SPLIT_THRESHOLD_KEY), e);
        }

        optionValue = options.get(MAX_SSTABLE_SIZE_THRESHOLD_KEY);
        try
        {
            long sstableSplitThreshold = optionValue == null ? DEFAULT_MAX_SSTABLE_SIZE_THRESHOLD : Long.parseLong(optionValue);
            if (sstableSplitThreshold < 0)
            {
                throw new ConfigurationException(String.format("%d must be greater than 0 for %s", sstableSplitThreshold, MAX_SSTABLE_SIZE_THRESHOLD_KEY));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, MAX_SSTABLE_SIZE_THRESHOLD_KEY), e);
        }

        optionValue = options.get(MIN_SSTABLE_SPLIT_WINDOW_KEY);
        try
        {
            int minSStableSplitWindow = optionValue == null ? DEFAULT_MIN_SSTABLE_SPLIT_WINDOW : Integer.parseInt(optionValue);
            if (minSStableSplitWindow < 0)
            {
                throw new ConfigurationException(String.format("%d must be greater than 0 for %s", minSStableSplitWindow, MIN_SSTABLE_SPLIT_WINDOW_KEY));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, MIN_SSTABLE_SPLIT_WINDOW_KEY), e);
        }
        uncheckedOptions.remove(COMPACTION_WINDOW_SIZE_KEY);
        uncheckedOptions.remove(COMPACTION_WINDOW_UNIT_KEY);
        uncheckedOptions.remove(TIMESTAMP_RESOLUTION_KEY);
        uncheckedOptions.remove(EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY);
        uncheckedOptions.remove(UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_KEY);
        uncheckedOptions.remove(SSTABLE_SPLIT_THRESHOLD_KEY);
        uncheckedOptions.remove(MAX_SSTABLE_SIZE_THRESHOLD_KEY);
        uncheckedOptions.remove(MIN_SSTABLE_SPLIT_WINDOW_KEY);

        uncheckedOptions = SizeTieredCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        return uncheckedOptions;
    }
}
