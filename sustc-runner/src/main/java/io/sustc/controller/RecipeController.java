package io.sustc.controller;

import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.service.RecipeService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/recipes")
public class RecipeController {
    private final RecipeService recipeService;

    public RecipeController(RecipeService recipeService) {
        this.recipeService = recipeService;
    }

    @GetMapping("/search")
    public PageResult<RecipeRecord> search(
            @RequestParam(defaultValue = "") String keyword,
            @RequestParam(required = false) String category,
            @RequestParam(required = false) Double minRating,
            @RequestParam(defaultValue = "1") Integer page,
            @RequestParam(defaultValue = "10") Integer size,
            @RequestParam(defaultValue = "date_desc") String sort
    ) {
        return recipeService.searchRecipes(keyword, category, minRating, page, size, sort);
    }

    @GetMapping("/{id}")
    public RecipeRecord getById(@PathVariable("id") long id) {
        return recipeService.getRecipeById(id);
    }
}

