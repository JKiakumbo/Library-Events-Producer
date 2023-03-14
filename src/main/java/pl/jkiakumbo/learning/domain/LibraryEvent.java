package pl.jkiakumbo.learning.domain;

public record LibraryEvent(Integer libraryEventId, LibraryEventType type, Book book) {

    public LibraryEvent setType(LibraryEventType type){
         return new LibraryEvent(libraryEventId,type, book);
     }
}
