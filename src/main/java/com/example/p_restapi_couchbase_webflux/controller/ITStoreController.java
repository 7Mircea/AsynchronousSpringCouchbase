package com.example.p_restapi_couchbase_webflux.controller;

import com.example.p_restapi_couchbase_webflux.model.Products;
import com.example.p_restapi_couchbase_webflux.repository.ProductsRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/itStore")
public class ITStoreController {
    ProductsRepository productsRepository;
    /*CharacteristicRepository characteristicRepository;
    CustomerEmployeeSupplierRepository customerEmployeeSupplierRepository;
    InvoiceRepository invoiceRepository;
    ItemRepository itemRepository;
    List<String> source;

    public ITStoreController(ProductsRepository productsRepository, CharacteristicRepository characteristicRepository, CustomerEmployeeSupplierRepository customerEmployeeSupplierRepository, InvoiceRepository invoiceRepository, ItemRepository itemRepository, List<String> source) {
        this.productsRepository = productsRepository;
        this.characteristicRepository = characteristicRepository;
        this.customerEmployeeSupplierRepository = customerEmployeeSupplierRepository;
        this.invoiceRepository = invoiceRepository;
        this.itemRepository = itemRepository;
        this.source = source;
    }

    @GetMapping("/products")
    public Iterable<Products> getAllProducts() {
        return productsRepository.findAll();
    }*/

    public ITStoreController(ProductsRepository productsRepository) {
        this.productsRepository = productsRepository;
    }

    @GetMapping("/first_N_products")
    public Flux<Products> getFirstNProducts(@RequestParam int N) {
        return productsRepository.findProductsByIdProdIsBefore(N);
    }

    /*@GetMapping("/product_max_profit")
    public Iterable<ProductsRepository.CategoryProductProfit> getProductWithMaxProfit() {
        return productsRepository.findProductWithGreatestProfitInCategory();
    }

    @GetMapping("/category_profit")
    public Iterable<ProductsRepository.CategoryProfit> getCategoryProfit() {
        return productsRepository.findProfitForEachCategory();
    }

    @GetMapping("/characteristics")
    public Iterable<Characteristic> getAllCharacteristics() {
        return characteristicRepository.findAll();
    }

    @GetMapping("/characteristics_for_product")
    public Iterable<Characteristic> getCharacteristicsForProduct(@RequestParam(name = "productId", required = true) Integer productId) {
        return characteristicRepository.findCharacteristicByProduct(productId);
    }

    @GetMapping("/customer_employee_suppliers")
    public Iterable<Customer_Employee_Supplier_Table> getAllCustomerEmployeeSupplier() {
        return customerEmployeeSupplierRepository.findAll();
    }

    @GetMapping("/employee")
    public Iterable<String> getEmployeeWithGreatestNrOfInvoices() {
        return customerEmployeeSupplierRepository.findEmployeeWithGreatestNrOfInvoices();
    }

    @GetMapping("/ces_by_type")
    public Iterable<Customer_Employee_Supplier_Table> getCustomerEmployeeSupplierByTypeCES(@RequestParam(name = "type", required = true) char type) {
        return customerEmployeeSupplierRepository.findCustomerEmployeeSupplierByTypeCES(type);
    }

    @GetMapping("/invoices")
    public Iterable<Invoice> getAllInvoices() {
        return invoiceRepository.findAll();
    }

    @GetMapping("/max_invoice")
    public Float getMaxInvoice() {
        return invoiceRepository.findMaxValue();
    }

    @GetMapping("/month_with_greatest_sale")
    public Iterable<InvoiceRepository.MonthSale> getMonthWithGreatestSale() {
        return invoiceRepository.getMonthWithGreatestSales();
    }

    @GetMapping("/sales_cost")
    public Set<InvoiceRepository.SalesCostMonthYear> getSalesCost() {
        return invoiceRepository.findSalesCostMonthYear();
    }

    @GetMapping("/profit_on_each_product")
    public Set<InvoiceRepository.ProfitOnEachProduct> getProfitOnEachProduct() {
        return invoiceRepository.findProfitOnEachProduct();
    }

    @GetMapping("/items")
    public Iterable<Item> getAllItems() {
        return itemRepository.findAll();
    }

    @GetMapping("/items_between_dates")
    public Iterable<ItemRepository.ProdQuantity1> getItemsBetweenDates() {
        return itemRepository.findItemsBetweenDates();
    }

    @GetMapping("/test_controller_only")
    public Iterable<String> testControllerOnly() {
        return source;
    }
*/
}
