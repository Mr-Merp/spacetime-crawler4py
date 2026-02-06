import re
import hashlib
from threading import RLock


class SimilarityTracker:    
    def __init__(self, exact_threshold=1.0, near_threshold=0.88, hash_bits=64):
        """
        Args:
            exact_threshold: Exact match threshold (1.0 = 100%)
            near_threshold: Simhash similarity threshold (0-1)
                           0.88 = 88% bit similarity = near-duplicate
            hash_bits: Number of bits in simhash fingerprint (default 64)
        """
        self.url_exact_hashes = {}     
        self.url_simhashes = {}         
        self.exact_threshold = exact_threshold
        self.near_threshold = near_threshold
        self.hash_bits = hash_bits
        self.lock = RLock()
    
    def _compute_exact_hash(self, text):
        """
        Compute exact MD5 hash of text for 100% duplicate detection.
        
        Returns: Hexadecimal hash string
        """
        return hashlib.md5(text.encode()).hexdigest()

    def _get_word_hash(self, word):
        """
        Generate a b-bit hash for a word using MD5.
        
        Returns an integer with b bits set according to the word's hash.
        """
        h = hashlib.md5(word.encode()).digest()
        hash_int = int.from_bytes(h[:8], byteorder='big')
        return hash_int & ((1 << self.hash_bits) - 1)

    def _extract_words(self, text):
        """
        Extract words from text with their frequencies.
        
        Returns a dictionary of {word: frequency}
        """
        if not text:
            return {}
        
        # Extract alphanumeric sequences
        words = re.findall(r'\b[a-z0-9]\b', text.lower())
        
        word_freqs = {}
        for word in words:
            word_freqs[word] = word_freqs.get(word, 0) + 1
        
        return word_freqs

    def _compute_simhash(self, text):
        """
        Compute Simhash fingerprint for text using word frequencies.

        Returns an integer representing the b-bit fingerprint.
        """
        word_freqs = self._extract_words(text)
        
        vector = [0] * self.hash_bits
        
        for word, freq in word_freqs.items():
            word_hash = self._get_word_hash(word)
            
            for i in range(self.hash_bits):
                bit = (word_hash >> i) & 1
                
                if bit == 1:
                    vector[i] += freq  
                else:
                    vector[i] -= freq  
        
        simhash = 0
        for i in range(self.hash_bits):
            if vector[i] > 0:
                simhash |= (1 << i)  
        
        return simhash

    def _hamming_similarity(self, hash1, hash2):
        """
        Calculate similarity as fraction of matching bits.
        
        Returns: Similarity score from 0.0 to 1.0
        """
        xor = hash1 ^ hash2
        
        hamming_distance = bin(xor).count('1')
        
        matching_bits = self.hash_bits - hamming_distance
        similarity = matching_bits / self.hash_bits
        
        return similarity

    def is_similar(self, url, page_text):
        """
        Check if page is similar to any stored page using both methods.
        
        Returns: 
            Tuple: (is_similar: bool, detection_method: str)
            - (True, 'exact') if exact duplicate found
            - (True, 'near') if near-duplicate found
            - (False, 'new') if page is new
        """
        if not page_text:# or len(page_text.strip()) < 50:
            return False, 'new'
        
        exact_hash = self._compute_exact_hash(page_text)
        simhash = self._compute_simhash(page_text)
        
        with self.lock:
            for stored_url, stored_exact_hash in self.url_exact_hashes.items():
                if exact_hash == stored_exact_hash:
                    return True, 'exact'
            
            for stored_url, stored_simhash in self.url_simhashes.items():
                similarity = self._hamming_similarity(simhash, stored_simhash)
                if similarity >= self.near_threshold:
                    return True, 'near'
            
            self.url_exact_hashes[url] = exact_hash
            self.url_simhashes[url] = simhash
            return False, 'new'

    def get_stats(self):
        """
        Return statistics about tracked pages and duplicates detected.
        
        Returns: Dictionary with tracking info
        """
        with self.lock:
            return {
                'unique_pages': len(self.url_simhashes),
                'exact_threshold': self.exact_threshold,
                'near_threshold': self.near_threshold,
                'hash_bits': self.hash_bits
            }