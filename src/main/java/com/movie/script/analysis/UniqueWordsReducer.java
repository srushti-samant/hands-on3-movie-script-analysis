package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class UniqueWordsReducer extends Reducer<Text, Text, Text, Text> {

    // To keep track of words and their associated characters across all keys
    private static HashMap<String, HashSet<String>> wordToCharacters = new HashMap<>();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Track words for the current character
        HashSet<String> characterWords = new HashSet<>();

        for (Text val : values) {
            String word = val.toString();
            characterWords.add(word);

            // Update the global word-to-character map
            if (!wordToCharacters.containsKey(word)) {
                wordToCharacters.put(word, new HashSet<>());
            }
            wordToCharacters.get(word).add(key.toString());
        }

        // After collecting all words for the current character, filter unique ones
        HashSet<String> uniqueWords = new HashSet<>();
        for (String word : characterWords) {
            // A word is unique if it's used by only one character
            if (wordToCharacters.get(word).size() == 1) {
                uniqueWords.add(word);
            }
        }

        // Emit the unique words for the current character
        if (!uniqueWords.isEmpty()) {
            context.write(key, new Text(String.join(", ", uniqueWords))); // Emit (CharacterName, UniqueWords)
        }
    }
}
