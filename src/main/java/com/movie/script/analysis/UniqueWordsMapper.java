package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class UniqueWordsMapper extends Mapper<Object, Text, Text, Text> {

    private Text character = new Text();
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line by ": " to separate character name from dialogue
        String line = value.toString();
        String[] parts = line.split(": ", 2); // Assumes "CharacterName: Dialogue" format

        if (parts.length == 2) {
            String characterName = parts[0].trim();
            String dialogue = parts[1].trim();

            // Tokenize the dialogue into individual words
            StringTokenizer itr = new StringTokenizer(dialogue);
            
            // Emit (CharacterName, Word)
            character.set(characterName);

            while (itr.hasMoreTokens()) {
                String wordText = itr.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase(); // Clean up and normalize the word
                if (!wordText.isEmpty()) {
                    word.set(wordText);
                    context.write(character, word); // Emit (CharacterName, Word)
                }
            }
        }
    }
}
