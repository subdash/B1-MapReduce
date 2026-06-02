import random
import os
import sys
import time

WORDS = [
    "the", "be", "to", "of", "and", "a", "in", "that", "have", "it",
    "for", "not", "on", "with", "he", "as", "you", "do", "at", "this",
    "but", "his", "by", "from", "they", "we", "say", "her", "she", "or",
    "an", "will", "my", "one", "all", "would", "there", "their", "what",
    "so", "up", "out", "if", "about", "who", "get", "which", "go", "me",
    "when", "make", "can", "like", "time", "no", "just", "him", "know",
    "take", "people", "into", "year", "your", "good", "some", "could",
    "them", "see", "other", "than", "then", "now", "look", "only", "come",
    "its", "over", "think", "also", "back", "after", "use", "two", "how",
    "our", "work", "first", "well", "way", "even", "new", "want", "because",
    "any", "these", "give", "day", "most", "us", "great", "between", "need",
    "large", "often", "hand", "high", "place", "hold", "turn", "were", "main",
    "move", "live", "where", "much", "through", "long", "down", "should",
    "every", "found", "still", "those", "tell", "too", "small", "might",
    "show", "kind", "example", "begin", "life", "always", "both", "paper",
    "together", "group", "run", "important", "until", "children", "side",
    "feet", "car", "mile", "night", "walk", "white", "sea", "began", "grow",
    "took", "river", "four", "carry", "state", "once", "book", "hear", "stop",
    "without", "second", "later", "miss", "idea", "enough", "eat", "face",
    "watch", "far", "real", "almost", "let", "above", "girl", "sometimes",
    "mountain", "cut", "young", "talk", "soon", "list", "song", "being",
    "leave", "family", "body", "music", "color", "stand", "sun", "questions",
    "fish", "area", "mark", "dog", "horse", "birds", "problem", "complete",
    "room", "knew", "since", "ever", "piece", "told", "usually", "friends",
    "easy", "heard", "order", "red", "door", "sure", "become", "top", "ship",
    "across", "today", "during", "short", "better", "best", "however", "low",
    "hours", "black", "products", "happened", "whole", "measure", "remember",
    "early", "waves", "reached", "listen", "wind", "rock", "space", "covered",
    "fast", "several", "himself", "toward", "five", "step", "morning", "passed",
    "vowel", "true", "hundred", "against", "pattern", "table", "north", "slowly",
    "money", "map", "farm", "pulled", "draw", "voice", "power", "town", "fine",
    "drive", "led", "cry", "dark", "machine", "note", "waited", "plan", "figure",
    "star", "box", "noun", "field", "rest", "correct", "able", "pound", "done",
    "beauty", "stood", "contain", "front", "teach", "week", "final", "gave",
    "green", "quick", "develop", "ocean", "warm", "free", "minute", "strong",
    "special", "behind", "clear", "tail", "produce", "fact", "street", "inch",
    "multiply", "nothing", "course", "stay", "wheel", "full", "force", "blue",
    "object", "decide", "surface", "deep", "moon", "island", "foot", "system",
    "busy", "test", "record", "boat", "common", "gold", "possible", "plane",
    "dry", "wonder", "laugh", "thousand", "ago", "ran", "check", "game",
    "shape", "hot", "brought", "heat", "snow", "tire", "bring", "yes", "fill",
    "east", "paint", "language", "among", "tree", "cross", "farm", "hard",
    "start", "might", "story", "saw", "far", "draw", "left", "late", "run",
    "while", "press", "close", "night", "real", "life", "few", "north", "open",
    "seem", "together", "next", "white", "children", "begin", "got", "walk",
    "example", "ease", "paper", "often", "always", "music", "those", "both",
    "mark", "book", "letter", "until", "mile", "river", "car", "feet", "care",
    "second", "enough", "plain", "girl", "usual", "young", "ready", "above",
    "ever", "red", "list", "though", "feel", "talk", "bird", "soon", "body",
    "dog", "family", "direct", "pose", "leave", "song", "measure", "door",
    "product", "black", "short", "numeral", "class", "wind", "question", "happen",
    "complete", "ship", "area", "half", "rock", "order", "fire", "south", "problem",
    "piece", "told", "knew", "pass", "since", "top", "whole", "king", "space",
    "heard", "best", "hour", "better", "true", "during", "hundred", "five",
    "remember", "step", "early", "hold", "west", "ground", "interest", "reach",
    "fast", "verb", "sing", "listen", "six", "table", "travel", "less", "morning",
    "ten", "simple", "several", "vowel", "toward", "war", "lay", "against",
    "pattern", "slow", "center", "love", "person", "money", "serve", "appear",
    "road", "map", "rain", "rule", "govern", "pull", "cold", "notice", "voice",
    "unit", "power", "town", "fine", "certain", "fly", "fall", "lead", "cry",
    "dark", "machine", "note", "wait", "plan", "figure", "star", "box", "noun",
    "field", "rest", "correct", "able", "pound", "done", "beauty", "drive",
    "contain", "front", "teach", "week", "final", "gave", "green", "oh", "quick",
    "develop", "ocean", "warm", "free", "minute", "strong", "special", "mind"
]

CHUNK = 50_000

# Seed the global RNG so every machine generates byte-identical sample-data.
# Override with: python3 scripts/generate_data.py <seed>
SEED = int(sys.argv[1]) if len(sys.argv) > 1 else 42
random.seed(SEED)

def generate_file(path, num_lines):
    start = time.time()
    with open(path, "wb") as f:
        remaining = num_lines
        while remaining > 0:
            batch = min(CHUNK, remaining)
            total_words = batch * 10
            selected = random.choices(WORDS, k=total_words)
            lines = []
            for j in range(batch):
                s = j * 10
                lines.append(b" ".join(w.encode() for w in selected[s:s+10]))
            f.write(b"\n".join(lines) + b"\n")
            remaining -= batch
    elapsed = time.time() - start
    size_mb = os.path.getsize(path) / 1_048_576
    print(f"  {num_lines:>9,} lines  {size_mb:6.1f} MB  {elapsed:5.1f}s")

os.makedirs("sample-data", exist_ok=True)

print(f"Generating 20 documents...\n{'doc':<20} {'lines':>9}  {'size':>6}  {'time':>5}")
print("-" * 50)

total_start = time.time()
for i in range(1, 21):
    num_lines = random.randint(700_000, 1_200_000)
    path = f"sample-data/document_{i:02d}.txt"
    print(f"document_{i:02d}.txt", end="", flush=True)
    generate_file(path, num_lines)

print(f"\nDone in {time.time() - total_start:.1f}s total.")
