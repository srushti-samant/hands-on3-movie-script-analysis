package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DialogueLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable wordCount = new IntWritable();
    private Text character = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line by ": " to separate character name from dialogue
        String line = value.toString();
        String[] parts = line.split(": ", 2); // Assumes "CharacterName: Dialogue" format

        if (parts.length == 2) {
            String characterName = parts[0].trim();
            String dialogue = parts[1].trim();

            // Tokenize the dialogue to count the words
            StringTokenizer itr = new StringTokenizer(dialogue);
            int wordCountInDialogue = 0;

            while (itr.hasMoreTokens()) {
                itr.nextToken();
                wordCountInDialogue++;
            }

            character.set(characterName);
            wordCount.set(wordCountInDialogue);

            // Emit (CharacterName, WordCount)
            context.write(character, wordCount);
        }
    }
}
