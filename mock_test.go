package goengine_test

import (
	"time"
)

type RecipeCreated struct {
	ocurredOn time.Time
}

func (e RecipeCreated) OcurredOn() time.Time {
	return e.ocurredOn
}

type RecipeRated struct {
	ocurredOn time.Time
	Rating    int
}

func (e RecipeRated) OcurredOn() time.Time {
	return e.ocurredOn
}

type Recipe struct {
	*AggregateRootBased
	Name   string
	Rating int
}

func NewRecipe(name string) *Recipe {
	recipe := new(Recipe)
	recipe.AggregateRootBased = NewAggregateRootBased(recipe)
	recipe.RecordThat(RecipeCreated{time.Now()})

	return recipe
}

func (r *Recipe) Rate(rate int) {
	r.RecordThat(RecipeRated{time.Now(), rate})
}

func (r *Recipe) WhenRecipeCreated(event RecipeCreated) {

}

func (r *Recipe) WhenRecipeRated(event RecipeRated) {
	r.Rating = event.Rating
}
