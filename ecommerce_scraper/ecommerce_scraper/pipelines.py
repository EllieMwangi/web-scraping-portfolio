import re
import logging
from itemadapter import ItemAdapter

logger = logging.getLogger(__name__)

class CleanProductDataPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        try:
            # Clean price fields
            if adapter.get("old_price"):
                adapter["old_price"] = self._extract_number(adapter["old_price"])
            if adapter.get("current_price"):
                adapter["current_price"] = self._extract_number(adapter["current_price"])

            # Clean discount percentage
            if adapter.get("discount_percent"):
                adapter["discount_percent"] = self._extract_number(adapter["discount_percent"])

            # Clean remaining stock
            if adapter.get("remaining_stock"):
                adapter["remaining_stock"] = self._extract_number(adapter["remaining_stock"])

            # Clean size (remove "Pack size :")
            if adapter.get("size"):
                adapter["size"] = re.sub(r"Pack size\s*:\s*", "", adapter["size"]).strip()

        except Exception as e:
            logger.error(f"Error cleaning item {item}: {e}", exc_info=True)

        return item

    def _extract_number(self, text):
        match = re.search(r"\d+", text.replace(",", "")) if isinstance(text, str) else None
        return int(match.group()) if match else text
