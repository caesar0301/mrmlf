About mrmlf
===========

This library provides an extended input format, namely **MultilineInputFormat**
based on new interfaces of Hadoop Mapreduce.

Data Target
-----------

Basically, default input formats in Hadoop are based on line-specific text
files or separater-aware binary formats (e.g., SequenceFile or avro format).
One of important data formats in real world is in the **multiple-line form**
where one integrated record is split into multiple natural lines (separated by
`\r` or `\n`). For exmpale, the music CD meta data in the illustrating program:

    <CD>
        <TITLE>Empire Burlesque</TITLE>
        <ARTIST>Bob Dylan</ARTIST>
        <COUNTRY>USA</COUNTRY>
        <COMPANY>Columbia</COMPANY>
        <PRICE>10.90</PRICE>
        <YEAR>1985</YEAR>
    </CD>
    <CD>
        <TITLE>Hide your heart</TITLE>
        <ARTIST>Bonnie Tyler</ARTIST>
        <COUNTRY>UK</COUNTRY>
        <COMPANY>CBS Records</COMPANY>
        <PRICE>9.90</PRICE>
        <YEAR>1988</YEAR>
    </CD>

This library manipulates the problem of this kind of data, and restores the
multiple-line content as a unified record for mapreduce programs. That is, the
CD metadata in the above example will be converted into two records.

Programming Integration
-----------------------

You can use this extended file input format as the way invoking other default
formats. Here we give a minimum configuration in MR program. The complete
sample source code is located at
`sjtu.omnilab.hadoop.mrtest.MultilineInputFormatSample.java` of the library.

    public final class MultilineInputFormatSample extends Configured {

        @Override
        public int run(String[] args) throws Exception {
            // Create a new MR job configuration
            job = ...

            // Invoke MLF class
            job.setInputFormatClass(MultilineInputFormat.class);
            MultilineInputFormat.setMultilineStartString(job, "<CD>");
            MultilineInputFormat.setMultilineEndString(job, "</CD>");

            // Set map output KV classes
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(TextArrayWritable.class);
            ...
        }
    }

More about Record Separation
----------------------------

There are two principle methods to determine the behavior of MLF, i.e.,
`MultilineInputFormat.setMultilineStartString` and
`MultilineInputFormat.setMultilineEndString`. The former tells the library
where the record beginning is and latter the record ending.

**Note:** Both methods are on the basis of natural lines and determine whether
  to split or not by matching the first characters in each line (including
  spacing characters like whitespaces and tabs).

Contact
-------

© Xiaming Chen -- chenxm35@gmail.com

License
-------

Apache License v2
