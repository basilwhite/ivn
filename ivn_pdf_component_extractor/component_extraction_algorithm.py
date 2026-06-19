import re
from typing import List, Dict, Optional, Tuple, Set

class AtomicComponentExtractor:
    def __init__(self, keywords: Optional[List[str]] = None):
        import spacy
        if keywords is None:
            keywords = ["must", "shall", "will", "should", "require", "goal", "priority", "standard"]
        self.keywords: List[str] = keywords
        self.initiative_phrases: List[str] = [
            "core human capital management", "core hcm", "federal hr 2.0", "advisory board", "transition", "platform", "initiative", "appendix", "agency actions", "agency transitions"
        ]
        self.nlp = spacy.load("en_core_web_sm")

    def synthesize_name(self, sentence: str, context: str) -> str:
        # Always prioritize initiative phrases if present
        for phrase in self.initiative_phrases:
            if phrase in context.lower() or phrase in sentence.lower():
                return phrase.title()
        # Extract action phrase (verb + object)
        action_match = re.search(r'\b([A-Z][a-z]+\s+(?:to\s+)?[a-zA-Z ]{10,60})', sentence)
        if action_match:
            return action_match.group(1).strip()
        # Fallback: use first 40 chars of context if not generic
        fallback = context.split(':')[0].strip() if ':' in context else context[:40]
        return fallback

    def is_generic(self, name: str) -> bool:
        # Filters out names that are just numbers, short headings, or generic terms
        if re.fullmatch(r'\d+(\.\d+)?', name.strip()):
            return True
        if name.strip().lower() in {"memorandum", "appendix", "introduction", "background"}:
            return True
        if len(name.strip()) < 20:
            return True
        if re.fullmatch(r'[A-Z][A-Z\s\d\-]+', name.strip()):
            return True
        if re.fullmatch(r'(\d+\.?)+', name.strip()):
            return True
        if re.match(r'^\d+\b', name.strip()) and len(name.strip()) < 30:
            return True
        # Exclude names that do not contain a verb or initiative phrase
        if not any(phrase in name.lower() for phrase in self.initiative_phrases) and not re.search(r'\b(must|shall|will|should|require|establish|transition|pause|evaluate|integrate|prepare|identify|do|act|board|platform|appendix|agency)\b', name, re.IGNORECASE):
            return True
        return False

    def extract(self, text: str) -> List[Dict[str, str]]:
        # Advanced approach: Use spaCy for sentence parsing, extract initiative phrases, action verbs, and noun chunks for context-rich names
        atomic_components: List[Tuple[str, str]] = []
        doc = self.nlp(text)
        for sent in doc.sents:
            sent_text = sent.text.strip()
            if len(sent_text) < 10:
                continue
            # Initiative phrase detection
            for phrase in self.initiative_phrases:
                if phrase in sent_text.lower():
                    atomic_components.append((phrase.title(), sent_text))
                    break
            # Action verb and noun chunk detection
            verbs = [token for token in sent if token.pos_ == "VERB" and token.dep_ in {"ROOT", "aux"}]
            noun_chunks = list(sent.noun_chunks)
            if verbs:
                for verb in verbs:
                    # Try to find the most relevant noun chunk (subject or object)
                    relevant_nouns = [chunk.text for chunk in noun_chunks if chunk.root.head == verb or chunk.root == verb]
                    if relevant_nouns:
                        name = f"{verb.lemma_.capitalize()} {' '.join(relevant_nouns)}"
                    else:
                        name = verb.lemma_.capitalize()
                    # Add the first 8 words of the sentence for more context
                    name = f"{name} {' '.join(sent_text.split()[:8])}"
                    atomic_components.append((name, sent_text))
                    break
            # Fallback: Use first 10 words as name if no verb or phrase found
            if not verbs and not any(phrase in sent_text.lower() for phrase in self.initiative_phrases):
                name = ' '.join(sent_text.split()[:10])
                atomic_components.append((name, sent_text))
        # Deduplicate and filter
        seen: Set[str] = set()
        result: List[Dict[str, str]] = []
        for name, desc in atomic_components:
            if name not in seen and len(desc) > 10 and not self.is_generic(name):
                seen.add(name)
                result.append({'name': name, 'description': desc})
        return result
