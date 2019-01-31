package goengine

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("A Recipe", func() {
	Describe("when someone rate it", func() {
		It("should have a recipe rated event", func() {
			recipe := NewRecipe("Test")
			recipe.Rate(5)

			Expect(recipe.Rating).To(Equal(5))
			Expect(len(recipe.GetUncommittedEvents())).To(Equal(2))
		})
	})
})
