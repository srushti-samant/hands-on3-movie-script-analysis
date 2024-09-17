package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text characterWord = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line by ": " to separate character name from dialogue
        String line = value.toString();
        String[] parts = line.split(": ", 2); // Assumes "CharacterName: Dialogue" format

        if (parts.length == 2) {
            String character = parts[0].trim();
            String dialogue = parts[1].trim();

            // Tokenize the dialogue into individual words
            StringTokenizer itr = new StringTokenizer(dialogue);

            while (itr.hasMoreTokens()) {
                String word = itr.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase(); // Normalize the word
                if (!word.isEmpty()) {
                    // Create a composite key "CharacterName:Word"
                    characterWord.set(character + ":" + word);
                    context.write(characterWord, one); // Emit (CharacterName:Word, 1)
                }
            }
        }
    }
}
