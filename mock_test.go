package goengine_test

import (
	"time"

	. "github.com/hellofresh/goengine"
)

type SomethingHappened struct {
	occurredOn time.Time
}

func NewSomethingHappened() SomethingHappened {
	return SomethingHappened{time.Now()}
}

func (e SomethingHappened) OccurredOn() time.Time {
	return e.occurredOn
}

type RecipeCreated struct {
	occurredOn time.Time
}

func (e RecipeCreated) OccurredOn() time.Time {
	return e.occurredOn
}

type RecipeRated struct {
	occurredOn time.Time
	Rating     int
}

func (e RecipeRated) OccurredOn() time.Time {
	return e.occurredOn
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
