use mcp_agent_mail_db::s3fifo::S3FifoCache;

fn main() {
    let mut cache = S3FifoCache::new(5);
    // Capacity 5 means Ghost capacity is 5, Small capacity is 1, Main is 4.

    // 1. Insert "a" -> goes to small.
    cache.insert("a", 1);
    
    // 2. Insert "b" -> evicts "a" from small (freq=0) -> goes to ghost.
    cache.insert("b", 2);
    // Now ghost has ["a"]. index["a"] = Ghost.

    // 3. Insert "a" -> is_ghost=true -> goes to main.
    // ghost STILL has ["a"]. index["a"] = Main.
    cache.insert("a", 3);

    // 4. Remove "a".
    // "a" is removed from main and index. 
    // ghost STILL has ["a"]. index has no "a".
    cache.remove(&"a");

    // 5. Insert "a" -> goes to small.
    cache.insert("a", 4);

    // 6. Insert "c" -> evicts "a" from small (freq=0) -> goes to ghost.
    // "a" is removed from index.
    // evict_ghost_if_full is called (but ghost only has ["a"], len=1 < 5, so no-op).
    // ghost.push_back("a") -> ghost has ["a", "a"].
    // index["a"] = Ghost.
    cache.insert("c", 5);

    // 7. Fill ghost queue to trigger evict_ghost_if_full.
    for i in 0..10 {
        let key = format!("k{}", i);
        cache.insert(key.clone(), i);
        // Insert a dummy to force eviction from small
        cache.insert(format!("dummy{}", i), i);
    }

    // Now, evict_ghost_if_full will pop the FIRST "a".
    // It will see index["a"] == Ghost.
    // It will REMOVE "a" from index.
    // But the SECOND "a" was supposed to be in the ghost queue!
    
    // So if we now try to insert "a" again, it won't be recognized as a ghost!
    cache.insert("a", 6);
    
    // Let's check where it went. If it was recognized as a ghost, it would go to Main.
    // If not, it goes to Small.
    assert_eq!(cache.small_len(), 1, "If a went to small, ghost was forgotten!");
    println!("Bug confirmed: 'a' was forgotten from Ghost because the first duplicate popped and cleared the index.");
}
