package io.sustc.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PageResult<T> implements Serializable {

    /**
     * List of items contained in the current paginated response.
     */
    private List<T> items;

    /**
     * Current page number (starting from 1).
     */
    private int page;

    /**
     * Number of items per page.
     */
    private int size;

    /**
     * Total number of records matching the query condition.
     */
    private long total;
}
